package com.bazaarvoice.emodb.local;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.bazaarvoice.emodb.web.EmoService;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EmoServiceWithZK {
    private static final ExecutorService service = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("EmbeddedCassandra-%d")
                    .build());

    public static void main(String... args) throws Exception {
        // Start cassandra if necessary (cassandra.yaml is provided)
        ArgumentParser parser = ArgumentParsers.newArgumentParser("java -jar emodb-web-local*.jar");
        parser.addArgument("server").required(true).help("server");
        parser.addArgument("emo-config").required(true).help("config.yaml - EmoDB's config file");
        parser.addArgument("emo-config-ddl").required(true).help("config-ddl.yaml - EmoDB's cassandra schema file");
        parser.addArgument("cassandra-yaml").nargs("?").help("cassandra.yaml - Cassandra configuration file to start an" +
                " in memory embedded Cassandra.");
        parser.addArgument("-z","--zookeeper").dest("zookeeper").action(Arguments.storeTrue()).help("Starts zookeeper");

        // Get the path to cassandraYaml or if zookeeper is available
        Namespace result = parser.parseArgs(args);
        String cassandraYaml = result.getString("cassandra-yaml");
        boolean startZk = result.getBoolean("zookeeper");

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

        try {
            EmoService.main(emoServiceArgs);
            success = true;

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
}
