package com.bazaarvoice.emodb.sdk;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.auth.util.ApiKeyEncryption;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;
import com.sun.jersey.api.client.Client;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.test.TestingServer;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.descriptor.PluginDescriptor;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.codehaus.plexus.util.FileUtils;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.twdata.maven.mojoexecutor.MojoExecutor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.twdata.maven.mojoexecutor.MojoExecutor.configuration;
import static org.twdata.maven.mojoexecutor.MojoExecutor.element;
import static org.twdata.maven.mojoexecutor.MojoExecutor.executeMojo;
import static org.twdata.maven.mojoexecutor.MojoExecutor.executionEnvironment;
import static org.twdata.maven.mojoexecutor.MojoExecutor.goal;

@Mojo(name = "start", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST)
public class EmoStartMojo extends AbstractEmoMojo {

    /** Computed. */
    private int cassandraStopPort;
    private int cassandraJmxPort;
    private int cassandraStoragePort;
    private int cassandraNativeTransportPort;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (skip) {
            getLog().info("Skipping emodb start...");
            return;
        }
        try {
            if (autoStartZookeeper) {
                startZookeeper();
            }
            if (autoStartCassandra) {
                startCassandra();
            }
            if (autoStartEmo) {
                installEmoFiles();
                startEmoAndWaitUntilHealthy();
            }
            initializeRolesAndApiKeys();
            if (waitForInterrupt) {
                sleepUntilInterrupted();
            }
        } catch (Exception e) {
            getLog().error(e);
        }
    }

    private void installEmoFiles() throws MojoExecutionException, MojoFailureException, IOException {
        copyEmoServiceArtifactToWorkingDirectory();
        copyEmoConfigurationFile();
        copyDdlConfigurationFile();
    }

    private void startEmoAndWaitUntilHealthy() throws MojoExecutionException, MojoFailureException, IOException {
        final EmoExec emoProcess = new EmoExec();
        emoProcess.setMaxMemoryMegabytes(emoMaxMemory);
        emoProcess.setDebugPort(emoDebugPort);
        emoProcess.setSuspendDebugOnStartup(suspendDebugOnStartup);
        if (null != emoLogFile) {
            emoProcess.setEmoLogFile(new File(emoLogFile));
        }

        emoProcess.execute(emoProcessWorkingDirectory(), getLog(), "server", "conf/config.yaml", "conf/config-ddl.yaml");

        CrossMojoState.addEmoProcess(emoProcess, getPluginContext());

        getLog().info("Waiting for healthy instance...");
        EmoHealthCondition.waitSecondsUntilHealthy(healthCheckPort, 60);
        getLog().info("healthy!");
    }

    private void copyEmoConfigurationFile() throws MojoExecutionException, IOException {
        if (StringUtils.isBlank(emoConfigurationFile)) {
            copyDefaultEmoConfigurationFile();
        } else {
            try {
                // copy configuration file to well-known emodb config directory and filename "config.yaml"
                FileUtils.copyFile(new File(emoConfigurationFile), new File(emoConfigurationDirectory(), "config.yaml"));
            } catch (Exception e) {
                throw new MojoExecutionException("failed to copy configuration file from " + emoConfigurationFile, e);
            }
        }
    }

    private void copyDdlConfigurationFile() throws MojoExecutionException, IOException {
        if (StringUtils.isBlank(ddlConfigurationFile)) {
            copyDefaultDdlConfigurationFile();
        } else {
            try {
                // copy configuration file to well-known emodb DDL config directory and filename "config-ddl.yaml"
                FileUtils.copyFile(new File(ddlConfigurationFile), new File(emoConfigurationDirectory(), "config-ddl.yaml"));
            } catch (Exception e) {
                throw new MojoExecutionException("failed to copy configuration file from " + ddlConfigurationFile, e);
            }
        }
    }

    private void copyEmoServiceArtifactToWorkingDirectory() throws MojoExecutionException, MojoFailureException {
        // NOTE: this emo service artifact is an "uberjar" created by the maven-shade-plugin
        final ArtifactItem emoServiceArtifact = new ArtifactItem();
        emoServiceArtifact.setGroupId("com.bazaarvoice.emodb");
        emoServiceArtifact.setArtifactId("emodb-web");
        emoServiceArtifact.setVersion(pluginVersion());

        resolveArtifactItems(asList(emoServiceArtifact));
        final File emoServiceJar = emoServiceArtifact.getResolvedArtifact().getArtifact().getFile();
        try {
            // copy to "emodb.jar" so that we always overwrite any prior
            FileUtils.copyFile(emoServiceJar, new File(emoProcessWorkingDirectory(), "emodb.jar"));
        } catch (IOException e) {
            throw new MojoFailureException("failed to copy: " + emoServiceArtifact, e);
        }
    }

    private void startZookeeper() {
        try {
            getLog().info("Starting zookeeper on port " + zookeeperPort);
            // ZooKeeper is too noisy.
            System.setProperty("org.slf4j.simpleLogger.log.org.apache.zookeeper", "error");
            final TestingServer zookeeperTestingServer = new TestingServer(zookeeperPort);

            CrossMojoState.putZookeeperTestingServer(zookeeperTestingServer, getPluginContext());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void startCassandra() throws MojoExecutionException {
        // find first available port and optimistically assume the subsequent ports are available.
        getLog().info("cassandraRpcPort = " + cassandraRpcPort);
        cassandraStopPort = cassandraRpcPort + 1;
        getLog().info("cassandraStopPort = " + cassandraStopPort);
        cassandraJmxPort = cassandraRpcPort + 2;
        getLog().info("cassandraJmxPort = " + cassandraJmxPort);
        cassandraStoragePort = cassandraRpcPort + 3;
        getLog().info("cassandraStoragePort = " + cassandraStoragePort);
        cassandraNativeTransportPort = cassandraRpcPort + 4;
        getLog().info("cassandraNativeTransportPort = " + cassandraNativeTransportPort);

        executeMojo(CASSANDRA_PLUGIN,
                goal("start"), cassandraStartConfiguration(),
                executionEnvironment(project, session, pluginManager)
        );

        CrossMojoState.putCassandraStopPort(cassandraStopPort, getPluginContext());
    }

    protected final Xpp3Dom cassandraStartConfiguration() {
        ImmutableList.Builder<MojoExecutor.Element> elementBuilder = ImmutableList.<MojoExecutor.Element>builder()
                .add(element("maxMemory", String.valueOf(cassandraMaxMemory)))
                .add(element("rpcPort", String.valueOf(cassandraRpcPort)))
                .add(element("stopPort", String.valueOf(cassandraStopPort)))
                .add(element("jmxPort", String.valueOf(cassandraJmxPort)))
                .add(element("storagePort", String.valueOf(cassandraStoragePort)))
                .add(element("nativeTransportPort", String.valueOf(cassandraNativeTransportPort)))
                .add(element("skip", String.valueOf(false)))
                .add(element("loadAfterFirstStart", String.valueOf(false)))
                .add(element("startNativeTransport", String.valueOf(true)))
                .add(element("yaml", "partitioner: org.apache.cassandra.dht.ByteOrderedPartitioner")); // random is the default
        if (cassandraDir != null) {
            elementBuilder.add(element("cassandraDir", cassandraDir));
        }
        List<MojoExecutor.Element> elements = elementBuilder.build();

        return configuration(elements.toArray(new MojoExecutor.Element[elements.size()]));
    }

    /** The version of this plugin used; is also the same version of the emodb server to use. */
    private String pluginVersion() {
        return ((PluginDescriptor) getPluginContext().get("pluginDescriptor")).getVersion();
    }

    private File emoProcessWorkingDirectory() {
        return ensureDirectory(new File(project.getBuild().getDirectory(), emoDir));
    }

    private File emoConfigurationDirectory() {
        return ensureDirectory(new File(emoProcessWorkingDirectory(), "conf"));
    }

    private List<ArtifactItem> resolveArtifactItems(List<ArtifactItem> artifactItems) throws MojoExecutionException {
        // resolved artifacts have been downloaded and are available locally
        for (ArtifactItem item : artifactItems) {
            try {
                item.setResolvedArtifact(repositorySystem.resolveArtifact(repositorySystemSession, toArtifactRequest(item)));
            } catch (ArtifactResolutionException e) {
                throw new MojoExecutionException("couldn't resolve: " + item, e);
            }
        }
        return artifactItems;
    }

    private ArtifactRequest toArtifactRequest(ArtifactItem item) {
        return new ArtifactRequest(toDefaultArtifact(item), project.getRemoteProjectRepositories(), "project");
    }

    private org.eclipse.aether.artifact.Artifact toDefaultArtifact(ArtifactItem item) {
        return new DefaultArtifact(item.getGroupId(), item.getArtifactId(), item.getClassifier(), item.getType()/*extension*/, item.getVersion());
    }

    private void copyDefaultEmoConfigurationFile() throws MojoExecutionException, IOException {
        InputStream source = null;
        FileOutputStream target = null;
        try {
            source = EmoStartMojo.class.getResourceAsStream("/emodb-default-config.yaml");
            target = new FileOutputStream(new File(emoConfigurationDirectory(), "config.yaml"));
            IOUtils.copy(source, target);
        } catch (IOException e) {
            throw new MojoExecutionException("could not find the default configuration file");
        } finally {
            Closeables.close(source, false);
            Closeables.close(target, false);
        }
    }

    private void copyDefaultDdlConfigurationFile() throws MojoExecutionException, IOException {
        InputStream source = null;
        FileOutputStream target = null;
        try {
            source = EmoStartMojo.class.getResourceAsStream("/emodb-default-config-ddl.yaml");
            target = new FileOutputStream(new File(emoConfigurationDirectory(), "config-ddl.yaml"));
            IOUtils.copy(source, target);
        } catch (IOException e) {
            throw new MojoExecutionException("could not find the ddl configuration file");
        } finally {
            Closeables.close(source, false);
            Closeables.close(target, false);
        }
    }

    private void sleepUntilInterrupted() throws IOException {
        getLog().info("Hit ENTER on the console to continue the build.");

        for (;;) {
            int ch = System.in.read();
            if (ch == -1 || ch == '\n') {
                break;
            }
        }
    }

    private void initializeRolesAndApiKeys() throws MojoFailureException {
        List<RoleParameter> roles = this.roles != null ? Arrays.asList(this.roles) : ImmutableList.<RoleParameter>of();
        List<ApiKeyParameter> apiKeys = this.apiKeys != null ? Arrays.asList(this.apiKeys) : ImmutableList.<ApiKeyParameter>of();

        if (roles.isEmpty() && apiKeys.isEmpty()) {
            // Nothing to create
            return;
        }

        Client client = Client.create();
        try {
            // Parse the config file to get the admin URI and API key.  To support as much forward compatibility as
            // possible deserialize the yaml into the minimum representation required.
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            MinimalEmoConfiguration config = objectMapper.readValue(new File(emoConfigurationDirectory(), "config.yaml"), MinimalEmoConfiguration.class);
            URI adminUri = getEmoUri(config);
            String adminApiKey = getAdminApiKey(config);

            for (RoleParameter role : roles) {
                String name = requireNonNull(role.getName(), "Role configuration must include a name");
                getLog().info("Creating role " + name);

                Map<String, Object> entity = ImmutableMap.of(
                        "name", role.getName(),
                        "permissions", ImmutableSet.copyOf(role.getPermissions()));

                String response = client.resource(adminUri)
                        .path("uac")
                        .path("1")
                        .path("role")
                        .path("_")
                        .path(name)
                        .queryParam("APIKey", adminApiKey)
                        .type("application/x.json-create-role")
                        .post(String.class, JsonHelper.asJson(entity));

                getLog().info("Response to create role " + name);
                getLog().info(response);
            }

            for (ApiKeyParameter apiKey : apiKeys) {
                String value = requireNonNull(apiKey.getValue(), "API key configuration must include a value");
                getLog().info("Creating API key " + value);

                Map<String, Object> entity = ImmutableMap.of(
                        "owner", "emodb-sdk",
                        "roles", Arrays.stream(apiKey.getRoles())
                                .map(role -> ImmutableMap.of("id", role))
                                .collect(Collectors.toList()));

                String response = client.resource(adminUri)
                        .path("uac")
                        .path("1")
                        .path("api-key")
                        .queryParam("APIKey", adminApiKey)
                        .queryParam("key", value)
                        .type("application/x.json-create-api-key")
                        .post(String.class, JsonHelper.asJson(entity));

                getLog().info("Response to create API key " + value);
                getLog().info(response);
            }
        } catch (Exception e) {
            throw new MojoFailureException("Failed to initialize roles and API keys", e);
        } finally {
            client.destroy();
        }
    }

    private URI getEmoUri(MinimalEmoConfiguration config) {
        return URI.create(String.format("http://localhost:%d", config.server.applicationConnectors.get(0).port));
    }

    private String getAdminApiKey(MinimalEmoConfiguration config) {
        return new ApiKeyEncryption(config.cluster).decrypt(config.auth.adminApiKey);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MinimalEmoConfiguration {
        public String cluster;
        public MinimalAuthConfiguration auth;
        public MinimalServerConfiguration server;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MinimalAuthConfiguration {
        public String adminApiKey;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MinimalServerConfiguration {
        public List<MinimalApplicationConnectorConfiguration> applicationConnectors;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MinimalApplicationConnectorConfiguration {
        public int port;
    }

}
