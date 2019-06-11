package com.bazaarvoice.emodb.web.megabus;

import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.bazaarvoice.emodb.common.dropwizard.guice.SystemTablePlacement;
import com.bazaarvoice.emodb.plugin.PluginServerMetadata;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.bazaarvoice.emodb.web.auth.ApiKeyEncryption;
import com.bazaarvoice.emodb.web.scanner.ScanUploadModule;
import com.bazaarvoice.emodb.web.scanner.ScanUploader;
import com.bazaarvoice.emodb.web.scanner.ScannerZooKeeper;
import com.bazaarvoice.emodb.web.scanner.control.DistributedScanRangeMonitor;
import com.bazaarvoice.emodb.web.scanner.control.MaxConcurrentScans;
import com.bazaarvoice.emodb.web.scanner.control.ScanUploadMonitor;
import com.bazaarvoice.emodb.web.scanner.control.ScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.notifications.MetricsScanCountListener;
import com.bazaarvoice.emodb.web.scanner.notifications.MetricsStashStateListener;
import com.bazaarvoice.emodb.web.scanner.notifications.ScanCountListener;
import com.bazaarvoice.emodb.web.scanner.rangescan.LocalRangeScanUploader;
import com.bazaarvoice.emodb.web.scanner.rangescan.RangeScanUploader;
import com.bazaarvoice.emodb.web.scanner.scanstatus.DataStoreScanStatusDAO;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatusDAO;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatusTable;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatusTablePlacement;
import com.bazaarvoice.emodb.web.scanner.writer.KafkaScanWriter;
import com.bazaarvoice.emodb.web.scanner.writer.ScanWriterGenerator;
import com.bazaarvoice.emodb.web.util.ZKNamespaces;
import com.bazaarvoice.megabus.MegabusBootConfiguration;
import com.bazaarvoice.megabus.MegabusBootDAO;
import com.bazaarvoice.megabus.MegabusZookeeper;
import com.google.common.base.Optional;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.dropwizard.setup.Environment;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MegabusStashModule extends PrivateModule {

    private final MegabusBootConfiguration _config;

    public MegabusStashModule(MegabusBootConfiguration configuration) {
        _config = checkNotNull(configuration);

        checkArgument(_config.getScanThreadCount() > 0, "Scan thread count must be at least 1");
    }

    @Override
    protected void configure() {
        binder().requireExplicitBindings();

        bind(ScanUploader.class).asEagerSingleton();
        bind(ScanStatusDAO.class).to(DataStoreScanStatusDAO.class).asEagerSingleton();
        bind(RangeScanUploader.class).to(LocalRangeScanUploader.class).asEagerSingleton();
        bind(ScanWorkflow.class).toProvider(ScanUploadModule.QueueScanWorkflowProvider.class).asEagerSingleton();

        bind(ScanCountListener.class).to(MetricsScanCountListener.class).asEagerSingleton();

        bind(String.class).annotatedWith(ScanStatusTable.class).toInstance(_config.getScanStatusTable());
        bind(Integer.class).annotatedWith(MaxConcurrentScans.class).toInstance(_config.getScanThreadCount());

        bind(new TypeLiteral<Optional<String>>(){}).annotatedWith(Names.named("pendingScanRangeQueueName"))
                .toInstance(Optional.of(_config.getPendingScanRangeQueueName()));
        bind(new TypeLiteral<Optional<String>>(){}).annotatedWith(Names.named("completeScanRangeQueueName"))
                .toInstance(Optional.of(_config.getCompleteScanRangeQueueName()));

        bind(ScanWriterGenerator.class).to(MegabusScanWriterGenerator.class).asEagerSingleton();

        install(new FactoryModuleBuilder()
                .implement(KafkaScanWriter.class, KafkaScanWriter.class)
                .build(KafkaScanWriterFactory.class));

        // Monitors active upload status, active only by leader election
        bind(ScanUploadMonitor.class).asEagerSingleton();
        // Monitors for new scans waiting to start
        bind(DistributedScanRangeMonitor.class).asEagerSingleton();

        bind(MegabusBootDAO.class).to(StashMegabusBootDAO.class).asEagerSingleton();
        expose(MegabusBootDAO.class);
    }

    @Provides
    @Singleton
    @ScanStatusTablePlacement
    protected String provideScanStatusTablePlacement(@SystemTablePlacement String tablePlacement) {
        return tablePlacement;
    }

    @Provides
    @Singleton
    @Named("ScannerAPIKey")
    protected String provideScannerApiKey(@ServerCluster String cluster) {
        checkArgument(_config.getQueueServiceApiKey() != null);
        String scannerApiKey = _config.getQueueServiceApiKey();
        if (ApiKeyEncryption.isPotentiallyEncryptedApiKey(scannerApiKey)) {
            scannerApiKey = new ApiKeyEncryption(cluster).decrypt(scannerApiKey);
        } else {
            LoggerFactory.getLogger("com.bazaarvoice.emodb.security").warn(
                    "Megabus Scanner API key is stored in plaintext; anyone with access to config.yaml can see it!!!");
        }
        return scannerApiKey;
    }

    @Provides
    @Singleton
    protected StashStateListener provideStashStateListener(Environment environment, PluginServerMetadata metadata) {
        MetricsStashStateListener listener = new MetricsStashStateListener();
        listener.init(environment, metadata, null);
        return listener;
    }

    @Provides
    @Singleton
    @ScannerZooKeeper
    CuratorFramework provideScannerZookeeper(@MegabusZookeeper CuratorFramework curatorFramework) {
        return ZKNamespaces.usingChildNamespace(curatorFramework, "boot");
    }
}
