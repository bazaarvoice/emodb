package com.bazaarvoice.emodb.sdk;

import org.apache.curator.test.TestingServer;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.codehaus.plexus.util.xml.Xpp3Dom;

import java.io.IOException;

import static org.twdata.maven.mojoexecutor.MojoExecutor.configuration;
import static org.twdata.maven.mojoexecutor.MojoExecutor.element;
import static org.twdata.maven.mojoexecutor.MojoExecutor.executeMojo;
import static org.twdata.maven.mojoexecutor.MojoExecutor.executionEnvironment;
import static org.twdata.maven.mojoexecutor.MojoExecutor.goal;

@Mojo(name = "stop", defaultPhase = LifecyclePhase.POST_INTEGRATION_TEST)
public final class EmoStopMojo extends AbstractEmoMojo {

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (skip) {
            return;
        }
        if (autoStartEmo) {
            stopEmo();
        }
        if (autoStartCassandra) {
            stopCassandra();
        }
        if (autoStartZookeeper) {
            stopZookeeper();
        }
    }

    private void stopEmo() {
        getLog().info("Stopping emodb service...");
        for (final EmoExec emoProcess : CrossMojoState.getEmoProcesses(getPluginContext())) {
            if (emoProcess != null) {
                emoProcess.destroy();
                emoProcess.waitFor();
            }
        }
    }

    private void stopZookeeper() {
        final TestingServer zookeeperTestingServer = CrossMojoState.getZookeeperTestingServer(getPluginContext());
        if (zookeeperTestingServer != null) {
            try {
                getLog().info("Stopping zookeeper...");
                zookeeperTestingServer.stop();
            } catch (IOException e) {
                /* best-effort */
            } finally {
                try {
                    zookeeperTestingServer.close();
                } catch (IOException e) {
                    /* best-effort */
                }
            }
        }
    }

    private void stopCassandra() throws MojoExecutionException {
        getLog().info("Stopping cassandra...");
        executeMojo(CASSANDRA_PLUGIN,
                goal("stop"), cassandraStopConfiguration(),
                executionEnvironment(project, session, pluginManager)
        );
    }

    private Xpp3Dom cassandraStopConfiguration() {
        return configuration(
                element("rpcPort", String.valueOf(cassandraRpcPort)),
                element("stopPort", String.valueOf(CrossMojoState.getCassandraStopPort(getPluginContext()))),
                element("skip", String.valueOf(false))
        );
    }

}