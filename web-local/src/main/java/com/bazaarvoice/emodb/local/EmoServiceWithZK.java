package com.bazaarvoice.emodb.local;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPortModule;
import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.bazaarvoice.emodb.common.json.CustomJsonObjectMapperFactory;
import com.bazaarvoice.emodb.uac.api.CreateEmoRoleRequest;
import com.bazaarvoice.emodb.uac.api.EmoRoleKey;
import com.bazaarvoice.emodb.uac.api.UpdateEmoRoleRequest;
import com.bazaarvoice.emodb.uac.api.UserAccessControl;
import com.bazaarvoice.emodb.uac.client.UserAccessControlClientFactory;
import com.bazaarvoice.emodb.uac.client.UserAccessControlFixedHostDiscoverySource;
import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.emodb.web.EmoService;
import com.bazaarvoice.emodb.web.auth.ApiKeyEncryption;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolProxies;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.dropwizard.server.ServerFactory;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.commons.lang.ArrayUtils;
import org.apache.curator.test.TestingServer;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EmoServiceWithZK {
    private static final ExecutorService service = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("EmbeddedCassandra-%d")
                    .build());

    public static void main(String... args) throws Exception {

        // Remove all nulls and empty strings from the argument list.  This can happen as if the maven command
        // starts the service with no permission YAML files.
        args = Arrays.stream(args).filter(arg -> !Strings.isNullOrEmpty(arg)).toArray(String[]::new);

        // Start cassandra if necessary (cassandra.yaml is provided)
        ArgumentParser parser = ArgumentParsers.newArgumentParser("java -jar emodb-web-local*.jar");
        parser.addArgument("server").required(true).help("server");
        parser.addArgument("emo-config").required(true).help("config.yaml - EmoDB's config file");
        parser.addArgument("emo-config-ddl").required(true).help("config-ddl.yaml - EmoDB's cassandra schema file");
        parser.addArgument("cassandra-yaml").nargs("?").help("cassandra.yaml - Cassandra configuration file to start an" +
                " in memory embedded Cassandra.");
        parser.addArgument("-z","--zookeeper").dest("zookeeper").action(Arguments.storeTrue()).help("Starts zookeeper");
        parser.addArgument("-p","--permissions-yaml").dest("permissions").nargs("*").help("Permissions file(s)");

        // Get the path to cassandraYaml or if zookeeper is available
        Namespace result = parser.parseArgs(args);
        String cassandraYaml = result.getString("cassandra-yaml");
        boolean startZk = result.getBoolean("zookeeper");
        String emoConfigYaml = result.getString("emo-config");
        List<String> permissionsYamls = result.getList("permissions");

        String[] emoServiceArgs = args;

        // Start ZooKeeper
        TestingServer zooKeeperServer = null;
        if (startZk) {
            zooKeeperServer = isLocalZooKeeperRunning() ? null : startLocalZooKeeper();
            emoServiceArgs = (String[]) ArrayUtils.removeElement(args, "-z");
            emoServiceArgs = (String[]) ArrayUtils.removeElement(emoServiceArgs, "--zookeeper");

        }
        boolean success = false;


        if (cassandraYaml != null) {
            // Replace $DIR$ so we can correctly specify location during runtime
            File templateFile = new File(cassandraYaml);
            String baseFile = Files.toString(templateFile, Charset.defaultCharset());
            // Get the jar location
            String path = EmoServiceWithZK.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            String parentDir = new File(path).getParent();
            String newFile = baseFile.replace("$DATADIR$", new File(parentDir, "data").getAbsolutePath());
            newFile = newFile.replace("$COMMITDIR$", new File(parentDir, "commitlog").getAbsolutePath());
            newFile = newFile.replace("$CACHEDIR$", new File(parentDir, "saved_caches").getAbsolutePath());
            File newYamlFile = new File(templateFile.getParent(), "emo-cassandra.yaml");
            Files.write(newFile, newYamlFile, Charset.defaultCharset());

            startLocalCassandra(newYamlFile.getAbsolutePath());
            emoServiceArgs = (String[]) ArrayUtils.removeElement(emoServiceArgs, cassandraYaml);
        }

        // If permissions files were configured remove them from the argument list
        int permissionsIndex = Math.max(ArrayUtils.indexOf(emoServiceArgs, "-p"), ArrayUtils.indexOf(emoServiceArgs, "--permissions-yaml"));
        if (permissionsIndex >= 0) {
            int permissionsArgCount = 1 + permissionsYamls.size();
            for (int i=0; i < permissionsArgCount; i++) {
                emoServiceArgs = (String[]) ArrayUtils.remove(emoServiceArgs, permissionsIndex);
            }
        }

        try {
            EmoService.main(emoServiceArgs);
            success = true;

            setPermissionsFromFiles(permissionsYamls, emoConfigYaml);
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            // The main web server command returns immediately--don't stop ZooKeeper/Cassandra in that case.
            if (zooKeeperServer != null && !(success && args.length > 0 && "server".equals(args[0]))) {
                zooKeeperServer.stop();
                service.shutdown();
            }
        }
    }

    /** Start an in-memory Cassandra. */
    private static void startLocalCassandra(String cassandraYamlPath) throws IOException {
        System.setProperty("cassandra.config", "file:" + cassandraYamlPath);
        final CassandraDaemon cassandra = new CassandraDaemon();
        cassandra.init(null);

        Futures.getUnchecked(service.submit(new Callable<Object>(){
            @Override
            public Object call() throws Exception
            {
                cassandra.start();
                return null;
            }
        }));
    }

    /** Start an in-memory copy of ZooKeeper. */
    private static TestingServer startLocalZooKeeper() throws Exception {
        // ZooKeeper is too noisy by default.
        ((Logger) LoggerFactory.getLogger("org.apache.zookeeper")).setLevel(Level.ERROR);

        // Start the testing server.
        TestingServer zooKeeperServer = new TestingServer(2181);

        // Configure EmoDB to use the testing server.
        System.setProperty("dw.zooKeeper.connectString", zooKeeperServer.getConnectString());

        return zooKeeperServer;
    }

    private static boolean isLocalZooKeeperRunning() {
        Socket socket = null;
        try {
            // Connect to a local ZooKeeper
            socket = new Socket("localhost", 2181);
            OutputStream out = socket.getOutputStream();

            // Send a 4-letter request
            out.write("ruok".getBytes());

            // Receive the 4-letter response
            byte[] response = new byte[4];
            ByteStreams.readFully(socket.getInputStream(), response);

            return Arrays.equals(response, "imok".getBytes());

        } catch (Throwable t) {
            return false;

        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }

    private static void setPermissionsFromFiles(List<String> permissionsYamls, String emoConfigYamlPath) {
        if (permissionsYamls.isEmpty()) {
            return;
        }

        ObjectMapper objectMapper = CustomJsonObjectMapperFactory.build(new YAMLFactory());
        EmoConfiguration emoConfig;

        try {
            emoConfig = objectMapper.readValue(new File(emoConfigYamlPath), EmoConfiguration.class);
        } catch (Exception e) {
            System.err.println("Failed to EmoDB configuration from file " + emoConfigYamlPath);
            e.printStackTrace(System.err);
            return;
        }

        final String cluster = emoConfig.getCluster();
        final MetricRegistry metricRegistry = new MetricRegistry();

        // Easiest path to get server port and API key decryptor for the admin API key is to use the same Guice injection
        // modules as the server.
        Module module = new AbstractModule() {
            @Override
            protected void configure() {
                bind(String.class).annotatedWith(ServerCluster.class).toInstance(cluster);
                bind(ServerFactory.class).toInstance(emoConfig.getServerFactory());
                bind(ApiKeyEncryption.class).asEagerSingleton();
                install(new SelfHostAndPortModule());
            }
        };

        Injector injector = Guice.createInjector(module);
        HostAndPort selfHostAndPort = injector.getInstance(Key.get(HostAndPort.class, SelfHostAndPort.class));
        ApiKeyEncryption apiKeyEncryption = injector.getInstance(ApiKeyEncryption.class);
        String adminApiKey = emoConfig.getAuthorizationConfiguration().getAdminApiKey();
        try {
            adminApiKey = apiKeyEncryption.decrypt(adminApiKey);
        } catch (Exception e) {
            if (ApiKeyEncryption.isPotentiallyEncryptedApiKey(adminApiKey)) {
                throw e;
            }
        }

        // Create a client for the local EmoDB service
        UserAccessControl uac = ServicePoolBuilder.create(UserAccessControl.class)
                .withHostDiscoverySource(new UserAccessControlFixedHostDiscoverySource("http://localhost:" + selfHostAndPort.getPort()))
                .withServiceFactory(UserAccessControlClientFactory.forCluster(cluster, metricRegistry).usingCredentials(adminApiKey))
                .withMetricRegistry(metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

        try {
            for (String permissionsYaml : permissionsYamls) {
                Map<String, List<String>> permissions;
                try {
                    permissions = objectMapper.readValue(new File(permissionsYaml), new TypeReference<Map<String, List<String>>>() {});
                } catch (Exception e) {
                    System.err.println("Failed to load permissions from file " + permissionsYaml);
                    e.printStackTrace(System.err);
                    return;
                }

                // Use the client to create or update all roles with permissions from the file
                for (Map.Entry<String, List<String>> entry : permissions.entrySet()) {
                    RoleIdentifier roleIdentifier = RoleIdentifier.fromString(entry.getKey());
                    EmoRoleKey roleKey = new EmoRoleKey(roleIdentifier.getGroup(), roleIdentifier.getId());
                    if (uac.getRole(roleKey) == null) {
                        uac.createRole(new CreateEmoRoleRequest(roleKey)
                                .setPermissions(ImmutableSet.copyOf(entry.getValue())));
                    } else {
                        uac.updateRole(new UpdateEmoRoleRequest(roleKey)
                                .grantPermissions(ImmutableSet.copyOf(entry.getValue())));
                    }
                }

            }
        } finally {
            ServicePoolProxies.close(uac);
        }
    }
}
