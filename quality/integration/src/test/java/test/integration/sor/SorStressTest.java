package test.integration.sor;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.client.DatabusAuthenticator;
import com.bazaarvoice.emodb.databus.client.DatabusClientFactory;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.client.DataStoreAuthenticator;
import com.bazaarvoice.emodb.sor.client.DataStoreClientFactory;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.bazaarvoice.ostrich.exceptions.OnlyBadHostsException;
import com.bazaarvoice.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolProxies;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.logging.LoggingFactory;
import org.apache.curator.framework.CuratorFramework;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validation;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class SorStressTest  {
    private static final Logger _log = LoggerFactory.getLogger(SorStressTest.class);

    private static final String TABLE = "stress";
    private static final String SUBSCRIPTION = "stress";
    private static final int MAX_WRITES = 10000000;

    private final DataStore _dataStore;
    private final Databus _databus;
    private final AtomicLong _numWrites = new AtomicLong();
    private final AtomicLong _numReads = new AtomicLong();
    private final AtomicLong _numIdle = new AtomicLong();
    private volatile boolean _done;

    public SorStressTest(DataStore dataStore, Databus databus) {
        _dataStore = dataStore;
        _databus = databus;
    }

    public void writeDeltas() {
        final Random random = new Random();
        final AtomicInteger count = new AtomicInteger();
        loop(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                if (count.get() >= MAX_WRITES) {
                    return false;
                }

                final Audit audit = new AuditBuilder().setLocalHost().build();
                _dataStore.updateAll(new Iterable<Update>() {
                    @Override
                    public Iterator<Update> iterator() {
                        return new AbstractIterator<Update>() {
                            @Override
                            protected Update computeNext() {
                                if (_done || count.getAndIncrement() == MAX_WRITES) {
                                    return endOfData();
                                }
                                sleep(1);
                                String key = randomString(random);
                                Map<String, Object> json = ImmutableMap.<String, Object>builder()
                                        .put("name", "Cassie")
                                        .put("rating", 5)
                                        .put("tags", ImmutableList.of("red", "seven", "shen"))
                                        .put("status", "APPROVED")
                                        .put("data", randomString(random))
                                        .build();
                                _numWrites.incrementAndGet();
                                return new Update(TABLE, key, TimeUUIDs.newUUID(), Deltas.literal(json), audit);
                            }
                        };
                    }
                });

                return false;
            }
        });
    }

    public void readDatabus() {
        loop(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Iterator<Event> events = _databus.poll(SUBSCRIPTION, Duration.standardSeconds(30), 10).getEventIterator();
                if (!events.hasNext()) {
                    _numIdle.incrementAndGet();
                    return false;  // idle
                }
                List<String> eventKeys = Lists.newArrayList();
                while (events.hasNext()) {
                    eventKeys.add(events.next().getEventKey());
                }
                _databus.acknowledge(SUBSCRIPTION, eventKeys);
                _numReads.addAndGet(eventKeys.size());
                return true;
            }
        });
    }

    public void report() {
        long prevWrites = 0, prevReads = 0, prevIdle = 0;
        while (!_done) {
            sleep(1000);
            long numWrites = _numWrites.get();
            long numReads = _numReads.get();
            long numIdle = _numIdle.get();

            _log.info(format("%,5d writes/s;\t%,5d reads/s;\t%,5d idle/s",
                    (numWrites - prevWrites),
                    (numReads - prevReads),
                    (numIdle - prevIdle)));
            prevWrites = numWrites;
            prevReads = numReads;
            prevIdle = numIdle;
        }
    }

    private void loop(Callable<Boolean> task) {
        while (!_done) {
            try {
                boolean idle = !task.call();

                if (!idle) {
                    sleep(100);
                }
            } catch (OnlyBadHostsException obhe) {
                _log.error("Only bad hosts.  Sleeping a while...");
                sleep(1000);

            } catch (Exception e) {
                _log.error("Exception polling the queue service.", e);
                sleep(100);
            }
        }
    }

    private void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // Ignore
        }
    }

    public void close() {
        _done = true;
    }

    private String randomString(Random random) {
        return new UUID(random.nextLong(), random.nextLong()).toString();
    }

    public static void main(String... args) throws Exception {
        final String DROPWIZARD_PROPERTY_PREFIX = "dw";

        // Load the config.yaml file specified as the first argument.
        ConfigurationFactory<EmoConfiguration> configFactory = new ConfigurationFactory(
                EmoConfiguration.class, Validation.buildDefaultValidatorFactory().getValidator(), Jackson.newObjectMapper(), DROPWIZARD_PROPERTY_PREFIX);
        EmoConfiguration configuration = configFactory.build(new File(args[0]));
        int numWriterThreads = Integer.parseInt(args[1]);
        int numReaderThreads = Integer.parseInt(args[2]);
        String apiKey = configuration.getAuthorizationConfiguration().getAdminApiKey();

        MetricRegistry metricRegistry = new MetricRegistry();
        new LoggingFactory().configure(metricRegistry, "stress");

        CuratorFramework curator = configuration.getZooKeeperConfiguration().newCurator();
        curator.start();

        DataStoreClientFactory dataStoreFactory = DataStoreClientFactory.forClusterAndHttpConfiguration(
                configuration.getCluster(), configuration.getHttpClientConfiguration(), metricRegistry);
        AuthDataStore authDataStore = ServicePoolBuilder.create(AuthDataStore.class)
                .withServiceFactory(dataStoreFactory)
                .withHostDiscovery(new ZooKeeperHostDiscovery(curator, dataStoreFactory.getServiceName(), metricRegistry))
                .withMetricRegistry(metricRegistry)
                .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
        DataStore dataStore = DataStoreAuthenticator.proxied(authDataStore).usingCredentials(apiKey);

        DatabusClientFactory databusFactory = DatabusClientFactory.forClusterAndHttpConfiguration(
                configuration.getCluster(), configuration.getHttpClientConfiguration(), metricRegistry);
        AuthDatabus authDatabus = ServicePoolBuilder.create(AuthDatabus.class)
                .withServiceFactory(databusFactory)
                .withHostDiscovery(new ZooKeeperHostDiscovery(curator, databusFactory.getServiceName(), metricRegistry))
                .withMetricRegistry(metricRegistry)
                .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
        Databus databus = DatabusAuthenticator.proxied(authDatabus).usingCredentials(apiKey);

        final SorStressTest stressTest = new SorStressTest(dataStore, databus);

        if (!dataStore.getTableExists(TABLE)) {
            TableOptions options = new TableOptionsBuilder().setPlacement("ugc_global:ugc").build();
            dataStore.createTable(TABLE, options, ImmutableMap.of("table", TABLE), new AuditBuilder().setLocalHost().build());
        }

        databus.subscribe(SUBSCRIPTION, Conditions.alwaysTrue(), Duration.standardDays(7), Duration.standardDays(1));

        ThreadFactory writerFactory = new ThreadFactoryBuilder().setNameFormat("SoR Writer-%d").build();
        for (int i = 0; i < numWriterThreads; i++) {
            writerFactory.newThread(new Runnable() {
                @Override
                public void run() {
                    stressTest.writeDeltas();
                }
            }).start();
        }

        ThreadFactory readerFactory = new ThreadFactoryBuilder().setNameFormat("Databus Reader-%d").build();
        for (int i = 0; i < numReaderThreads; i++) {
            readerFactory.newThread(new Runnable() {
                @Override
                public void run() {
                    stressTest.readDatabus();
                }
            }).start();
        }

        ThreadFactory reportFactory = new ThreadFactoryBuilder().setNameFormat("Report-%d").build();
        Executors.newScheduledThreadPool(1, reportFactory).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                stressTest.report();
            }
        }, 1, 1, TimeUnit.SECONDS);

        ServicePoolProxies.close(dataStore);
        Closeables.close(curator, true);
    }
}
