package com.bazaarvoice.emodb.web.migrator;


import com.bazaarvoice.emodb.plugin.stash.MigratorStateListener;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.emodb.web.migrator.config.MigratorConfiguration;
import com.bazaarvoice.emodb.web.migrator.migratorstatus.MigratorStatusDAO;
import com.bazaarvoice.emodb.web.migrator.migratorstatus.MigratorStatusTable;
import com.bazaarvoice.emodb.web.migrator.migratorstatus.MigratorStatusTablePlacement;
import com.bazaarvoice.emodb.web.scanner.control.ScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatusDAO;
import com.google.inject.*;

import static com.google.common.base.Preconditions.checkArgument;


public class MigratorModule extends PrivateModule {
    private final MigratorConfiguration _config;

    public MigratorModule(EmoConfiguration config) {
        _config = config.getMigrator().get();

        checkArgument(_config.getReadThreadCount() > 0, "Read thread count must be at least 1");
        checkArgument(_config.getWriteThreadCount() > 0, "Write thread count must be at least 1");
    }

    @Override
    protected void configure() {
        binder().requireExplicitBindings();

        bind(ScanStatusDAO.class).to(MigratorStatusDAO.class).asEagerSingleton();
        bind(ScanWorkflow.class).to(MigratorWorkflow.class).asEagerSingleton();
        bind(StashStateListener.class).to(MigratorStateListener.class).asEagerSingleton();
        bind(String.class).annotatedWith(MigratorStatusTable.class).toInstance(_config.getMigrateStatusTable());
        bind(String.class).annotatedWith(MigratorStatusTablePlacement.class).toInstance(_config.getMigrateStatusTablePlacement());


        bind(DeltaMigrator.class).asEagerSingleton();
        expose(DeltaMigrator.class);
    }
//
//    @Override
//    protected void configure() {
//        binder().requireExplicitBindings();
//
//        bind(MigratorWriter.class).asEagerSingleton();
//        bind(MigratorStatusDAO.class).asEagerSingleton();
////        bind(RangeScanUploader.class).to(LocalRangeScanUploader.class).asEagerSingleton();
//
////        bind(MetricsReadCountListener.class).asEagerSingleton();
//
////        Class<? extends Provider<? extends ScanWorkflow>> scanWorkflowProvider = QueueScanWorkflowProvider.class;
//        bind(MigratorWorkflow.class).toProvider(scanWorkflowProvider).asEagerSingleton();
//
//        bind(String.class).annotatedWith(MigratorStatusTable.class).toInstance(_config.getScanStatusTable());
//        bind(Integer.class).annotatedWith(MaxConcurrentScans.class).toInstance(_config.getScanThreadCount());
//
//        bind(new TypeLiteral<Optional<String>>(){}).annotatedWith(Names.named("pendingReadRangeQueueName"))
//                .toInstance(_config.getPendingReadRangeQueueName());
//        bind(new TypeLiteral<Optional<String>>(){}).annotatedWith(Names.named("completeScanRangeQueueName"))
//                .toInstance(_config.getCompleteReadRangeQueueName());
//
//        bind(ScanWriterGenerator.class).asEagerSingleton();
//        install(new FactoryModuleBuilder()
//                .implement(FileScanWriter.class, FileScanWriter.class)
//                .implement(S3ScanWriter.class, S3ScanWriter.class)
//                .implement(DiscardingScanWriter.class, DiscardingScanWriter.class)
//                .build(ScanWriterFactory.class));
//
//        // Monitors active upload status, active only by leader election
//        bind(ScanUploadMonitor.class).asEagerSingleton();
//        // Monitors for new scans waiting to start
//        bind(DistributedScanRangeMonitor.class).asEagerSingleton();
//        // Schedules any daily scans
//        bind(ScanUploadSchedulingService.class).asEagerSingleton();
//        // Sends notification that the service is active at the start of a scheduled scan
//        bind(ScanParticipationService.class).asEagerSingleton();
//
//        expose(ScanUploader.class);
//    }
//
//    @Provides
//    @Singleton
//    @MigratorWriteService
//    protected ScheduledExecutorService provideWriteExecutorService(Environment environment) {
//        return environment.lifecycle().scheduledExecutorService("MigratorWrite-%d") //TODO: this string may be significant and incorrect
//                .threads(_config.getWriteThreadCount()).build();
//    }
//
////    @Provides
////    @Singleton
////    protected MetricsStashStateListener provideMetricsStashStateListener(Environment environment, PluginServerMetadata metadata) {
////        MetricsStashStateListener listener = new MetricsStashStateListener();
////        listener.init(environment, metadata, null);
////        return listener;
////    }
//
////    @Provides
////    @Singleton
////    @Named ("plugin")
////    protected List<StashStateListener> providePluginStashStateListeners(Environment environment, PluginServerMetadata metadata) {
////        List<PluginConfiguration> pluginConfigs = _config.getNotifications().getStashStateListenerPluginConfigurations();
////        if (pluginConfigs.isEmpty()) {
////            return ImmutableList.of();
////        }
////
////        ImmutableList.Builder<StashStateListener> listeners = ImmutableList.builder();
////        for (PluginConfiguration config : pluginConfigs) {
////            final StashStateListener listener = PluginInstanceGenerator.generateInstance(
////                    config.getClassName(), StashStateListener.class, config.getConfig(),
////                    environment, metadata);
////            listeners.add(listener);
////        }
////
////        return listeners.build();
////    }
//
////    @Provides
////    @Singleton
////    protected MigratorStateListener provideMigratorStateListener(MetricsStashStateListener metricsListener,
////                                                           Optional<SNSStashStateListener> snsListener,
////                                                           @Named("plugin") List<StashStateListener> pluginListeners) {
////        List<StashStateListener> listeners = Lists.newArrayListWithCapacity(3);
////        listeners.add(metricsListener);
////        if (snsListener.isPresent()) {
////            listeners.add(snsListener.get());
////        }
////        listeners.addAll(pluginListeners);
////        return MultiStashStateListener.combine(listeners);
////    }
//
//
//    @Provides
//    @Singleton
//    @Named ("MigratorApiKey")
//    protected String provideMigratorApiKey(@ServerCluster String cluster) {
//        if (!_config.getMigratorApiKey().isPresent()) {
//            return "anonymous";
//        }
//        String migratorApiKey = _config.getMigratorApiKey().get();
//        if (ApiKeyEncryption.isPotentiallyEncryptedApiKey(migratorApiKey)) {
//            migratorApiKey = new ApiKeyEncryption(cluster).decrypt(migratorApiKey);
//        } else {
//            LoggerFactory.getLogger("com.bazaarvoice.emodb.security").warn(
//                    "Migrator API key is stored in plaintext; anyone with access to config.yaml can see it!!!");
//        }
//        return migratorApiKey;
//    }
//
//    /** Provider used internally when EmoDB queues are configured */
//    public static class QueueScanWorkflowProvider implements Provider<MigratorWorkflow> {
//        private final CuratorFramework _curator;
//        private final String _cluster;
//        private final Client _client;
//        private final String _apiKey;
//        private final Environment _environment;
//        private final MetricRegistry _metricRegistry;
//        private final String _pendingReadRangeQueueName;
//        private final String _completeReadRangeQueueName;
//
//        @Inject
//        public QueueScanWorkflowProvider(@Global CuratorFramework curator, @ServerCluster String cluster,
//                                         Client client, @Named ("ScannerAPIKey") String apiKey,
//                                         @Named ("pendingReadRangeQueueName") Optional<String> pendingReadRangeQueueName,
//                                         @Named ("completeReadRangeQueueName") Optional<String> completeReadRangeQueueName,
//                                         Environment environment, MetricRegistry metricRegistry) {
//            _curator = curator;
//            _cluster = cluster;
//            _client = client;
//            _apiKey = apiKey;
//            _environment = environment;
//            _metricRegistry = metricRegistry;
//            _pendingReadRangeQueueName = pendingReadRangeQueueName.or("emodb-pending-read-ranges");
//            _completeReadRangeQueueName = completeReadRangeQueueName.or("emodb-complete-read-ranges");
//        }
//
//        @Override
//        public MigratorWorkflow get() {
//            QueueClientFactory factory = QueueClientFactory.forClusterAndHttpClient(_cluster, _client);
//
//            // Don't use the local queue service; create a client to call out to the live EmoDB application.
//            AuthQueueService authService = ServicePoolBuilder.create(AuthQueueService.class)
//                    .withServiceFactory(factory)
//                    .withHostDiscovery(new ZooKeeperHostDiscovery(_curator, factory.getServiceName(), _metricRegistry))
//                    .withMetricRegistry(_metricRegistry)
//                    .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
//
//            _environment.lifecycle().manage(new ManagedServicePoolProxy(authService));
//
//            QueueService service = QueueServiceAuthenticator.proxied(authService)
//                    .usingCredentials(_apiKey);
//
//            return new MigratorWorkflow(service, _pendingReadRangeQueueName, _completeReadRangeQueueName);
//        }
//    }
}
