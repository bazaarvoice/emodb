package com.bazaarvoice.emodb.web.migrator;


import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.emodb.queue.client.QueueClientFactory;
import com.bazaarvoice.emodb.queue.client.QueueServiceAuthenticator;
import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.emodb.web.auth.ApiKeyEncryption;
import com.bazaarvoice.emodb.web.migrator.config.MigratorConfiguration;
import com.bazaarvoice.emodb.web.migrator.migratorstatus.MigratorStatusDAO;
import com.bazaarvoice.emodb.web.migrator.migratorstatus.MigratorStatusTable;
import com.bazaarvoice.emodb.web.migrator.migratorstatus.MigratorStatusTablePlacement;
import com.bazaarvoice.emodb.web.scanner.control.MaxConcurrentScans;
import com.bazaarvoice.emodb.web.scanner.control.QueueScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.control.ScanWorkflow;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.bazaarvoice.ostrich.dropwizard.pool.ManagedServicePoolProxy;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.inject.*;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.sun.jersey.api.client.Client;
import io.dropwizard.setup.Environment;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;


public class MigratorModule extends PrivateModule {
    private final MigratorConfiguration _config;

    public MigratorModule(EmoConfiguration config) {
        _config = config.getDeltaMigrator().get();

        checkArgument(_config.getReadThreadCount() > 0, "Read thread count must be at least 1");
        checkArgument(_config.getMaxWritesPerSecond() > 0, "Max writes per second must be at least 1");
        checkArgument(_config.getMigratorSplitSize() > 0, "Migrator split size must be at least 1");

    }

    @Override
    protected void configure() {
        binder().requireExplicitBindings();

        bind(DeltaMigrator.class).asEagerSingleton();
        bind(MigratorStatusDAO.class).asEagerSingleton();
        bind(MigratorRateLimiter.class).to(MigratorStatusDAO.class);
        bind(LocalRangeMigrator.class).asEagerSingleton();

        bind(ScanWorkflow.class).toProvider(QueueScanWorkflowProvider.class).asEagerSingleton();

        bind(String.class).annotatedWith(MigratorStatusTable.class).toInstance(_config.getMigrateStatusTable());
        bind(String.class).annotatedWith(MigratorStatusTablePlacement.class).toInstance(_config.getMigrateStatusTablePlacement());
        bind(Integer.class).annotatedWith(MaxConcurrentScans.class).toInstance(_config.getReadThreadCount());

        bind(new TypeLiteral<Optional<String>>(){}).annotatedWith(Names.named("pendingMigrationRangeQueueName"))
                .toInstance(_config.getPendingMigrationRangeQueueName());
        bind(new TypeLiteral<Optional<String>>(){}).annotatedWith(Names.named("completeMigrationRangeQueueName"))
                .toInstance(_config.getCompleteMigrationRangeQueueName());
        bind(Integer.class).annotatedWith(Names.named("maxWritesPerSecond"))
                .toInstance(_config.getMaxWritesPerSecond());

        bind(Integer.class).annotatedWith(MigratorSplitSize.class).toInstance(_config.getMigratorSplitSize());

        bind(MigratorMonitor.class).asEagerSingleton();
        bind(DistributedMigratorRangeMonitor.class).asEagerSingleton();

        expose(DeltaMigrator.class);
    }

    @Provides
    @Singleton
    @Named ("MigratorAPIKey")
    protected String provideMigratorApiKey(@ServerCluster String cluster) {
        if (!_config.getMigrateApiKey().isPresent()) {
            return "anonymous";
        }
        String migratorApiKey = _config.getMigrateApiKey().get();
        if (ApiKeyEncryption.isPotentiallyEncryptedApiKey(migratorApiKey)) {
            migratorApiKey = new ApiKeyEncryption(cluster).decrypt(migratorApiKey);
        } else {
            LoggerFactory.getLogger("com.bazaarvoice.emodb.security").warn(
                    "Migrator API key is stored in plaintext; anyone with access to config.yaml can see it!!!");
        }
        return migratorApiKey;
    }

    public static class QueueScanWorkflowProvider implements Provider<ScanWorkflow> {
        private final CuratorFramework _curator;
        private final String _cluster;
        private final Client _client;
        private final String _apiKey;
        private final Environment _environment;
        private final MetricRegistry _metricRegistry;
        private final String _pendingScanRangeQueueName;
        private final String _completeScanRangeQueueName;

        @Inject
        public QueueScanWorkflowProvider(@Global CuratorFramework curator, @ServerCluster String cluster,
                                         Client client, @Named("MigratorAPIKey") String apiKey,
                                         @Named ("pendingMigrationRangeQueueName") Optional<String> pendingMigrationRangeQueueName,
                                         @Named ("completeMigrationRangeQueueName") Optional<String> completeMigrationRangeQueueName,
                                         Environment environment, MetricRegistry metricRegistry) {
            _curator = curator;
            _cluster = cluster;
            _client = client;
            _apiKey = apiKey;
            _environment = environment;
            _metricRegistry = metricRegistry;
            _pendingScanRangeQueueName = pendingMigrationRangeQueueName.or("emodb-pending-migrator-ranges");
            _completeScanRangeQueueName = completeMigrationRangeQueueName.or("emodb-complete-migrator-ranges");
        }

        @Override
        public ScanWorkflow get() {
            QueueClientFactory factory = QueueClientFactory.forClusterAndEmoClient(_cluster, new JerseyEmoClient(_client));

            // Don't use the local queue service; create a client to call out to the live EmoDB application.
            AuthQueueService authService = ServicePoolBuilder.create(AuthQueueService.class)
                    .withServiceFactory(factory)
                    .withHostDiscovery(new ZooKeeperHostDiscovery(_curator, factory.getServiceName(), _metricRegistry))
                    .withMetricRegistry(_metricRegistry)
                    .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

            _environment.lifecycle().manage(new ManagedServicePoolProxy(authService));

            QueueService service = QueueServiceAuthenticator.proxied(authService)
                    .usingCredentials(_apiKey);

            return new QueueScanWorkflow(service, _pendingScanRangeQueueName, _completeScanRangeQueueName);
        }
    }
}
