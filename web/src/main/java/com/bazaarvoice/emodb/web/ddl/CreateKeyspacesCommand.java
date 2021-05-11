package com.bazaarvoice.emodb.web.ddl;

import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.common.json.CustomJsonObjectMapperFactory;
import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.emodb.web.util.EmoServiceObjectMapperFactory;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.io.Closeables;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ConfigurationValidationException;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.bazaarvoice.emodb.web.ddl.DdlConfiguration.Keyspace;
import static com.bazaarvoice.emodb.web.ddl.DdlConfiguration.Keyspaces;
import static java.lang.String.format;

/**
 * Updates the Cassandra schema with the keyspaces required by EmoDB.
 * <p>
 * The first time this is run, against an clean, empty Cassandra, it will create keyspaces and column families and
 * configure them to replicate to current data center only (specified via the <code>--data-center</code> argument).
 * <p>
 * The next time this is run, if from another data center, it will modify the replication settings of the existing
 * keyspaces to include the new data center.
 */
public final class CreateKeyspacesCommand extends ConfiguredCommand<EmoConfiguration> {
    private static final Logger _log = LoggerFactory.getLogger(CreateKeyspacesCommand.class);

    private static final Duration LOCK_ACQUIRE_TIMEOUT = Duration.ofSeconds(5);
    private static final int HR = 100;
    private static Validator _validator = Validation.buildDefaultValidatorFactory().getValidator();

    // Assume NetworkTopologyStrategy that supports both single and multiple data centers.
    private static final String STRATEGY_CLASS = "NetworkTopologyStrategy";

    private boolean _outputOnly;

    public CreateKeyspacesCommand() {
        super("create-keyspaces", "Create the keyspaces for Emo/SoR, Media/Blob, or Databus (only if the keyspaces do not yet exist).");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        subparser.addArgument("--data-center").required(true).help("Cassandra data center to update");
        subparser.addArgument("-o", "--output-only").action(Arguments.storeTrue())
                .help("only write the ddl to standard output");
        subparser.addArgument("config-ddl").required(true).help("config-ddl.yaml");
    }

    @Override
    protected void run(Bootstrap<EmoConfiguration> bootstrap, Namespace namespace, EmoConfiguration emoConfiguration)
            throws Exception {
        _outputOnly = namespace.getBoolean("output_only");

        DdlConfiguration ddlConfiguration = parseDdlConfiguration(toFile(namespace.getString("config-ddl")));
        CuratorFramework curator = null;
        if (!_outputOnly) {
            curator = emoConfiguration.getZooKeeperConfiguration().newCurator();
            curator.start();
        }
        try {
            createKeyspacesIfNecessary(emoConfiguration, ddlConfiguration, curator, bootstrap.getMetricRegistry());

        } finally {
            Closeables.close(curator, true);
        }
    }

    public void createKeyspacesIfNecessary(EmoConfiguration emoConfiguration, DdlConfiguration ddlConfiguration,
                                           CuratorFramework curator, MetricRegistry metricRegistry) {
        createKeyspaces(emoConfiguration.getDataStoreConfiguration().getCassandraClusters(),
                ddlConfiguration.getSystemOfRecord(), "/ddl/sor", emoConfiguration, curator, metricRegistry);

        createKeyspaces(emoConfiguration.getBlobStoreConfiguration().getCassandraClusters(),
                ddlConfiguration.getBlobStore(), "/ddl/media", emoConfiguration, curator, metricRegistry);

        createKeyspaces(emoConfiguration.getDatabusConfiguration().getCassandraConfiguration(),
                ddlConfiguration.getDatabus(), "/ddl/databus", emoConfiguration, curator, metricRegistry);

        createKeyspaces(emoConfiguration.getQueueConfiguration().getCassandraConfiguration(),
                ddlConfiguration.getQueue(), "/ddl/queue", emoConfiguration, curator, metricRegistry);
    }

    public static DdlConfiguration parseDdlConfiguration(File file) throws IOException, ConfigurationException {
        // Similar to Dropwizard's ConfigurationFactory but ignores System property overrides.
        ObjectMapper mapper = EmoServiceObjectMapperFactory.build(new YAMLFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        DdlConfiguration ddlConfiguration = mapper.readValue(file, DdlConfiguration.class);

        Set<ConstraintViolation<DdlConfiguration>> errors = _validator.validate(ddlConfiguration);
        if (!errors.isEmpty()) {
            throw new ConfigurationValidationException(file.toString(), errors);
        }

        return ddlConfiguration;
    }

    private void createKeyspaces(Map<String, CassandraConfiguration> keyspaceConfigs,
                                 Keyspaces keyspacesMetadata, String ddlResourcePath, EmoConfiguration conf,
                                 CuratorFramework curator, MetricRegistry metricRegistry) {
        for (CassandraConfiguration keyspaceConfig : keyspaceConfigs.values()) {
            createKeyspaces(keyspaceConfig, keyspacesMetadata, ddlResourcePath, conf, curator, metricRegistry);
        }
    }

    private void createKeyspaces(CassandraConfiguration keyspaceConfig,
                                 Keyspaces keyspacesMetadata, String ddlResourcePath, EmoConfiguration conf,
                                 CuratorFramework curator, final MetricRegistry metricRegistry) {
        for (String keyspaceName : keyspaceConfig.getKeyspaces().keySet()) {
            createKeyspace(keyspaceName, keyspaceConfig, keyspacesMetadata, ddlResourcePath,
                    conf, curator, metricRegistry);
        }
    }

    private void createKeyspace(final String keyspaceName, final CassandraConfiguration keyspaceConfig,
                                Keyspaces keyspacesMetadata, String ddlResourcePath, final EmoConfiguration conf,
                                final CuratorFramework curator, final MetricRegistry metricRegistry) {
        // Lookup the annotations provided by config-ddl.yaml
        final Keyspace keyspaceMetadata = keyspacesMetadata.getKeyspaces().get(keyspaceName);
        if (keyspaceMetadata == null) {
            throw new RuntimeException(format("Ddl configuration did not specify keyspace: %s", keyspaceName));
        }

        final String dataCenter = conf.getDataCenterConfiguration().getCassandraDataCenter();
        String emoCluster = conf.getCluster();

        final List<String> createCfCqlScripts = Lists.newArrayList();
        for (Map<String, String> tableMetadata : keyspaceMetadata.getTables().values()) {
            String cql = CqlTemplate.create(ddlResourcePath + "/tables.template.cql")
                    .withBindings(tableMetadata)
                    .toCqlScript();
            assertNoExtantVariablesInTemplate(cql);
            createCfCqlScripts.add(cql);
        }

        if (_outputOnly) {
            printCreateKeyspace(keyspaceName, dataCenter,
                    keyspaceMetadata.getReplicationFactor(), createCfCqlScripts);
        } else {
            // Execute the update in cluster-wide mutex to avoid race conditions.
            inMutex(curator, "/applications/emodb/" + emoCluster + "/auto-ddl", new Runnable() {
                @Override
                public void run() {
                    try (CassandraThriftFacade cassandra = toCassandraClient(keyspaceConfig, curator, metricRegistry)) {
                        updateKeyspaceInDataCenter(cassandra, keyspaceName,
                                dataCenter, keyspaceMetadata.getReplicationFactor(), createCfCqlScripts);
                    }
                }
            });
        }
    }

    private void printCreateKeyspace(String keyspace, String dataCenter, int replicationFactor,
                                     List<String> createCfCqlScripts) {
        // Generate the CQL equivalent to the systemAddKeyspace call we'd do in an actual update.
        System.out.println(Strings.repeat("-", HR));
        System.out.printf("CREATE KEYSPACE %s WITH replication = {%n", keyspace);
        System.out.printf("  'class': '%s'%n,", STRATEGY_CLASS);
        System.out.printf("  '%s': %d%n", dataCenter, replicationFactor);
        System.out.printf("};%n");

        for (String tableCql : createCfCqlScripts) {
            System.out.println(Strings.repeat("-", HR));
            System.out.println(tableCql);
            System.out.println(Strings.repeat("-", HR));
        }
    }

    private static void inMutex(CuratorFramework curator, String mutexPath, Runnable work) {
        final InterProcessMutex mutex = new InterProcessMutex(curator, mutexPath);
        try {
            // try to acquire mutex for index within flush period
            if (mutex.acquire(LOCK_ACQUIRE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                try {
                    work.run();
                } finally {
                    mutex.release();
                }
            } else {
                _log.warn("could not acquire index lock after {} millis!!", LOCK_ACQUIRE_TIMEOUT.toMillis());
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private void updateKeyspaceInDataCenter(CassandraThriftFacade cassandra, String keyspace,
                                            String dataCenter, int replicationFactor,
                                            List<String> createCfCqlScripts) {
        // Temporary hack for backward compatibility with Cassandra 1.1, fix CREATE TABLE statements to use
        // the old-style compression parameter syntax.
        boolean beforeCassandra12 = isBeforeCassandra12(cassandra);
        if (beforeCassandra12) {
            createCfCqlScripts = Lists.transform(createCfCqlScripts, new Function<String, String>() {
                @Override
                public String apply(String cql) {
                    // Eg.: compression = {'sstable_compression' : 'DeflateCompressor'}
                    //  ->: compression_parameters:sstable_compression = 'DeflateCompressor'
                    return cql.replaceAll(
                            "compression\\s*=\\s*\\{\\s*'(\\w+)'\\s*:\\s*'(\\w*)'\\s*\\}",
                            "compression_parameters:$1 = '$2'");
                }
            });
        }

        KsDef ksDef = cassandra.describeKeyspace(keyspace);
        boolean ksDefWasNull = false;

        if (ksDef == null) {
            ksDefWasNull = true;
            _log.info("Creating new keyspace '{}' in '{}' and creating column families.", keyspace, dataCenter);
            ksDef = new KsDef(keyspace, STRATEGY_CLASS, Collections.<CfDef>emptyList());
            ksDef.setStrategy_options(ImmutableMap.of(dataCenter, Integer.toString(replicationFactor)));
            cassandra.systemAddKeyspace(ksDef);
        }
        // Create the column families using CQL
        for (String cql : createCfCqlScripts) {
            cassandra.executeCql3Script(cql);
        }
        if (!ksDefWasNull) {
            if (!ksDef.getStrategy_options().containsKey(dataCenter)) {
                // Add the data center to the replication topology.
                _log.info("Updating keyspace '{}' to replicate to '{}'.", keyspace, dataCenter);
                Map<String, String> strategyOptions = Maps.newLinkedHashMap(ksDef.getStrategy_options());
                strategyOptions.put(dataCenter, Integer.toString(replicationFactor));
                ksDef.setStrategy_options(strategyOptions);
                ksDef.setCf_defs(Collections.<CfDef>emptyList());  // Don't modify column family definitions--assume they're correct already.
                cassandra.systemUpdateKeyspace(ksDef);

            } else {
                _log.info("Not modifying keyspace '{}' since it already includes '{}'.", keyspace, dataCenter);
            }
        }
    }

    private CassandraThriftFacade toCassandraClient(CassandraConfiguration cassandraConfiguration,
                                                    CuratorFramework curator, MetricRegistry metricRegistry) {
        cassandraConfiguration.withZooKeeperHostDiscovery(curator);
        cassandraConfiguration.performHostDiscovery(metricRegistry);
        return CassandraThriftFacade.forSeedsAndPort(cassandraConfiguration.getSeeds(), cassandraConfiguration.getThriftPort());
    }

    private static void assertNoExtantVariablesInTemplate(String value) {
        if (value.contains("${")) {
            throw new RuntimeException("not all variables were substituted: " + value);
        }
    }

    private static boolean isBeforeCassandra12(CassandraThriftFacade cassandra) {
        // Cassandra 1.1.12 reports thrift version 19.33.0, Cassandra 1.2.0 (beta) reports 19.34.0.
        String thriftVersion = cassandra.describeVersion();
        return Ordering.<Integer>natural().lexicographical().compare(
                Iterables.transform(Splitter.on('.').split(thriftVersion), new Function<String, Integer>() {
                    @Override
                    public Integer apply(String field) {
                        return Integer.parseInt(field);
                    }
                }),
                ImmutableList.of(19, 34)
        ) < 0;
    }

    private static File toFile(String path) {
        return new File(path);
    }
}
