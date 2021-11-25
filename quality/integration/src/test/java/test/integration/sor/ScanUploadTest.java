package test.integration.sor;

import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.stash.StashUtil;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.client.DataStoreClientFactory;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.emodb.web.EmoService;
import com.bazaarvoice.emodb.web.guice.SelfHostAndPortModule;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.bazaarvoice.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolProxies;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.sun.jersey.api.client.Client;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.server.SimpleServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.curator.framework.CuratorFramework;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * This test is too lengthy to run as a unit test, but it is useful to verify that the Stash is working as
 * an end-to-end process.  This test runs in three parts:
 *
 * 1.  Create and populate numerous tables of varying sizes
 * 2.  Start a scan upload process into a provided directory
 * 3.  Verify the contents written to that directory are consistent
 *
 * Prerequisite:
 *
 * Cassandra, ZooKeeper, and EmoDB must be running on the local machine.
 *
 * To execute on commandline:
 *
 * java -cp <ScanUploadTest.class> <path/to/config.yaml> <path/to/config-ddl.yaml> <output/directory>
 *
 * You may have better luck with IDEA since you may get into class path issues on command line:
 * Main Class: test.integration.sor.ScanUploadTest
 * Program Arguments: <path/to/config.yaml> <output/directory>
 *
 */
public class ScanUploadTest {
    private static final Logger _log = LoggerFactory.getLogger(ScanUploadTest.class);

    private final DataStore _dataStore;

    public ScanUploadTest(DataStore dataStore) {
        _dataStore = dataStore;
    }

    public void createAndPopulateTestTables() {
        // Use 10 threads to write tables with varying sizes from 0 to 100,000 rows
        ExecutorService populationService = Executors.newFixedThreadPool(10);
        final TableOptions options = new TableOptionsBuilder().setPlacement("ugc_global:ugc").build();

        List<Future<?>> futures = Lists.newArrayList();

        for (int rows=0; rows <= 100000; rows += 5000) {
            final int t = rows;

            Future<?> future = populationService.submit(new Runnable() {
                @Override
                public void run() {
                    String tableName = format("scan_upload_test:%d", t);
                    _log.info("Populating table {}", tableName);

                    if (!_dataStore.getTableExists(tableName)) {
                        _dataStore.createTable(tableName, options, ImmutableMap.of("count", String.valueOf(t)), new AuditBuilder().setLocalHost().build());
                    }

                    for (int r=0; r < t; r++) {
                        String key = format("key:%05d", r);
                        _dataStore.update(tableName, key, TimeUUIDs.newUUID(),
                                Deltas.mapBuilder()
                                        .put("row", r)
                                        .put("some_random_number", new Random(r + t).nextInt())
                                        .put("some_animal", "cow")
                                        .put("some_city", "Norfolk")
                                        .put("some_philosopher", "Nietzsche")
                                        .put("some_insult", "Your mother smells of elderberries")
                                        .build(),
                                new AuditBuilder().setLocalHost().build(),
                                WriteConsistency.STRONG);
                    }

                    _log.info("Populating table {} complete", tableName);
                }
            });

            futures.add(future);
        }

        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            populationService.shutdownNow();
        }
    }

    public void scanToDirectory(EmoConfiguration config, String ddlConfigPath, File configFile, File dir, HostAndPort hostAndPort, ValidatorFactory validatorFactory, MetricRegistry metricRegistry) throws Exception {
        // Run the ScanUploadCommand in process.  To do this we need to create a DropWizard Environment.
        Environment environment = new Environment("emodb", Jackson.newObjectMapper(), validatorFactory.getValidator(),
                metricRegistry, ClassLoader.getSystemClassLoader());

        Server server = null;
        try {
            // Start the server
            EmoService emoService = new EmoService(ddlConfigPath, configFile);
            emoService.initialize(new Bootstrap<>(emoService));
            emoService.run(config, environment);
            server = config.getServerFactory().build(environment);
            server.start();

            String scanId = String.format("scan%d", System.currentTimeMillis());

            // Use the API to start the scan and upload
            Client client = new JerseyClientBuilder(environment).build("scanUploadTest");
            ScanStatus scanStatus = client.resource(String.format("http://localhost:%d/stash/1/job/%s", hostAndPort.getPort(), scanId))
                    .queryParam("placement", "ugc_global:ugc")
                    .queryParam("dest", dir.toURI().toString())
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .post(ScanStatus.class, null);

            assertNotNull(scanStatus);
            assertEquals(scanStatus.getScanId(), scanId);

            // Continuously poll the API until the upload is complete
            boolean complete = false;
            while (!complete) {
                Thread.sleep(5000);

                scanStatus = client.resource(URI.create(String.format("http://localhost:%d/stash/1/job/%s", hostAndPort.getPort(), scanId)))
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .get(ScanStatus.class);

                complete = scanStatus.isDone();
            }
        } finally {
            if (server != null) {
                try {
                    server.stop();
                } catch (Exception e) {
                    _log.warn("Failed to stop server", e);
                }
            }
        }
    }

    public void validateScanResults(File dir) throws Exception {
        assertFalse(new File(dir, "scan_upload_test-0").exists());

        for (int rows=5000; rows <= 100000; rows += 5000) {
            String tableName = format("scan_upload_test:%d", rows);
            _log.info("Validating scan of table {}", tableName);

            File tableDir = new File(dir, StashUtil.encodeStashTable(tableName));
            assertTrue(tableDir.isDirectory());

            BitSet rowsScanned = new BitSet(rows);

            //noinspection ConstantConditions
            for (File shardFile : tableDir.listFiles()) {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(shardFile))))) {
                    String json;
                    while ((json = in.readLine()) != null) {
                        //noinspection unchecked
                        Map<String, Object> map = JsonHelper.fromJson(json, Map.class);

                        int row = (Integer) map.get("row");
                        assertTrue(row >= 0 && row < rows);
                        assertFalse(rowsScanned.get(row));
                        rowsScanned.set(row);

                        Map<String, Object> actual = _dataStore.get(tableName, format("key:%05d", row));
                        assertEquals(map, actual, "Scan table does not match Emo record");
                    }
                }
            }

            assertEquals(rowsScanned.nextClearBit(0), rows);

            _log.info("Validating scan of table {} complete", tableName);
        }
    }

    public static void main(String args[]) throws Exception {
        ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory();
        // Load the config.yaml file specified as the first argument.
        ConfigurationFactory<EmoConfiguration> configFactory = new ConfigurationFactory<>(
                EmoConfiguration.class, validatorFactory.getValidator(), Jackson.newObjectMapper(),
                "dw");
        File configFile = new File(args[0]);
        EmoConfiguration configuration = configFactory.build(configFile);
        String ddlFilePath = args[1];
        File scanUploadDir = new File(args[2]);

        checkArgument(scanUploadDir.isDirectory(), "Not a valid directory: %s", scanUploadDir);
        checkArgument(configuration.getServiceMode() == EmoServiceMode.SCANNER,
                "Not configured for scanner: %s", configuration.getServiceMode());

        // To prevent conflicting with EmoDB running on this same server adjust the host and admin ports.
        updatePortsToAvoidCollision(configuration.getServerFactory());

        HostAndPort hostAndPort = new SelfHostAndPortModule().provideSelfHostAndPort(configuration.getServerFactory());
        MetricRegistry metricRegistry = new MetricRegistry();
        configuration.getLoggingFactory().configure(metricRegistry, "scan");

        DataStore dataStore = null;

        try (CuratorFramework curator = configuration.getZooKeeperConfiguration().newCurator()) {
            curator.start();

            DataStoreClientFactory dataStoreFactory = DataStoreClientFactory.forClusterAndHttpConfiguration(
                    configuration.getCluster(), configuration.getHttpClientConfiguration(), metricRegistry);
            dataStore = ServicePoolBuilder.create(DataStore.class)
                    .withServiceFactory(dataStoreFactory.usingCredentials(configuration.getScanner().get().getScannerApiKey().get()))
                    .withHostDiscovery(new ZooKeeperHostDiscovery(curator, dataStoreFactory.getServiceName(), metricRegistry))
                    .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                    .withMetricRegistry(metricRegistry)
                    .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

            ScanUploadTest test = new ScanUploadTest(dataStore);
            test.createAndPopulateTestTables();
            test.scanToDirectory(configuration, ddlFilePath, configFile, scanUploadDir, hostAndPort, validatorFactory, metricRegistry);
            test.validateScanResults(scanUploadDir);
        } finally {
            if (dataStore != null) {
                ServicePoolProxies.close(dataStore);
            }
        }
    }

    private static void updatePortsToAvoidCollision(ServerFactory serverFactory) {
        if (serverFactory instanceof DefaultServerFactory) {
            DefaultServerFactory defaultServerFactory = (DefaultServerFactory)serverFactory;
            updatePortsToAvoidCollision(defaultServerFactory.getApplicationConnectors());
            updatePortsToAvoidCollision(defaultServerFactory.getAdminConnectors());
        } else if (serverFactory instanceof SimpleServerFactory) {
            SimpleServerFactory simpleServerFactory = (SimpleServerFactory)serverFactory;
            updatePortsToAvoidCollision(Collections.singleton(simpleServerFactory.getConnector()));
        } else {
            throw new IllegalStateException("Encountered an unexpected ServerFactory type");
        }
    }

    private static void updatePortsToAvoidCollision(Collection<ConnectorFactory> connectorFactoryCollection) {
        for (ConnectorFactory connectorFactory : connectorFactoryCollection) {
            if (connectorFactory instanceof HttpConnectorFactory) {
                HttpConnectorFactory httpConnectorFactory = (HttpConnectorFactory)connectorFactory;
                httpConnectorFactory.setPort(httpConnectorFactory.getPort() + 100);
            }
        }
    }
}
