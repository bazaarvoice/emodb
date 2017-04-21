package com.bazaarvoice.emodb.sdk;

import org.apache.maven.execution.MavenSession;
import org.apache.maven.model.Plugin;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.BuildPluginManager;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.twdata.maven.mojoexecutor.MojoExecutor.artifactId;
import static org.twdata.maven.mojoexecutor.MojoExecutor.groupId;
import static org.twdata.maven.mojoexecutor.MojoExecutor.plugin;
import static org.twdata.maven.mojoexecutor.MojoExecutor.version;

public abstract class AbstractEmoMojo extends AbstractMojo {

    @Component
    protected RepositorySystem repositorySystem;

    @Parameter(property = "session.repositorySession", required = true, readonly = true)
    protected RepositorySystemSession repositorySystemSession;

    @Component
    protected MavenProject project;

    @Component
    protected MavenSession session;

    @Component
    protected BuildPluginManager pluginManager;

    @Parameter(defaultValue = "false")
    protected boolean skip;

    @Parameter(defaultValue = "false", property = "emo.waitForInterrupt")
    protected boolean waitForInterrupt;

    @Parameter(defaultValue = "true", property = "emo.autoStartEmo")
    protected boolean autoStartEmo;

    @Parameter(defaultValue = "true", property = "emo.autoStartCassandra")
    protected boolean autoStartCassandra;

    @Parameter(defaultValue = "false", property = "emo.autoStartZookeeper")
    protected boolean autoStartZookeeper;

    @Parameter(property = "emo.zookeeperPort", defaultValue = "2181")
    protected int zookeeperPort;

    /** From this port we will compute the other ports. */
    @Parameter(property = "emo.cassandraRpcPort", defaultValue = "9160")
    protected int cassandraRpcPort;

    @Parameter(defaultValue = "8061", property = "emo.healthCheckPort")
    protected int healthCheckPort;

    @Parameter(property = "emo.cassandraMavenVersion")
    protected String cassandraMavenVersion;

    @Parameter(property = "emo.cassandraMaxMemory", defaultValue = "1000")
    protected int cassandraMaxMemory; // megabytes

    @Parameter(property = "emo.cassandraDir")
    protected String cassandraDir;

    @Parameter(property = "emo.emoDir", defaultValue = "emodb")
    protected String emoDir;

    @Parameter
    protected String emoConfigurationFile;

    @Parameter
    protected String ddlConfigurationFile;

    @Parameter(property = "emo.maxMemory", defaultValue = "1000")
    protected int emoMaxMemory; // megabytes

    @Parameter(property = "emo.debugPort", defaultValue = "-1")
    protected int emoDebugPort;

    @Parameter
    protected RoleParameter[] roles;

    @Parameter
    protected ApiKeyParameter[] apiKeys;

    @Parameter(property = "debug.suspend")
    protected boolean suspendDebugOnStartup = false;

    @Parameter(required = false)
    protected String emoLogFile;


    /** We'll expect to find a resource in *this* package that tells us the cassandra maven version to use. */
    private static final String EMO_MAVEN_PLUGIN_PROPERTIES_RESOURCE = "/emo-maven-plugin.properties";

    private static final Properties EMO_MAVEN_PLUGIN_PROPERTIES = loadEmoMavenPluginProperties();

    protected static final Plugin CASSANDRA_PLUGIN = plugin(
            groupId("com.bazaarvoice.maven.plugins"),
            artifactId("cassandra-maven-plugin"),
            version(requireEmoMavenPluginProperty("cassandra-maven-plugin.version"))
    );

    protected static File ensureDirectory(File dir) {
        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new RuntimeException("couldn't create directories: " + dir);
        }
        return dir;
    }

    private static Properties loadEmoMavenPluginProperties() {
        final Properties properties = new Properties();
        try {
            final InputStream ourPluginProperties = AbstractEmoMojo.class.getResourceAsStream(EMO_MAVEN_PLUGIN_PROPERTIES_RESOURCE);
            if (ourPluginProperties == null) {
                throw new IllegalStateException("couldn't find " + EMO_MAVEN_PLUGIN_PROPERTIES_RESOURCE + "on the classpath");
            }
            properties.load(ourPluginProperties);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;
    }

    private static String requireEmoMavenPluginProperty(String key) {
        final String value = EMO_MAVEN_PLUGIN_PROPERTIES.getProperty(key);
        if (value == null || "".equals(value.trim())) {
            throw new IllegalStateException("expected to find '" + key + "' in " + EMO_MAVEN_PLUGIN_PROPERTIES_RESOURCE);
        }
        return value;
    }

}
