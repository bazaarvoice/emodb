package com.bazaarvoice.emodb.web.scanner;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.bazaarvoice.emodb.common.stash.StashUtil;
import com.bazaarvoice.emodb.plugin.PluginConfiguration;
import com.bazaarvoice.emodb.plugin.PluginServerMetadata;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.bazaarvoice.emodb.plugin.util.PluginInstanceGenerator;
import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.emodb.queue.client.QueueClientFactory;
import com.bazaarvoice.emodb.queue.client.QueueServiceAuthenticator;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.common.dropwizard.guice.SystemTablePlacement;
import com.bazaarvoice.emodb.web.auth.ApiKeyEncryption;
import com.bazaarvoice.emodb.web.scanner.config.ScannerConfiguration;
import com.bazaarvoice.emodb.web.scanner.config.ScheduledScanConfiguration;
import com.bazaarvoice.emodb.web.scanner.control.DistributedScanRangeMonitor;
import com.bazaarvoice.emodb.web.scanner.control.MaxConcurrentScans;
import com.bazaarvoice.emodb.web.scanner.control.QueueScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.control.SQSScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.control.ScanUploadMonitor;
import com.bazaarvoice.emodb.web.scanner.control.ScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.notifications.CloudWatchScanCountListener;
import com.bazaarvoice.emodb.web.scanner.notifications.MetricsScanCountListener;
import com.bazaarvoice.emodb.web.scanner.notifications.MetricsStashStateListener;
import com.bazaarvoice.emodb.web.scanner.notifications.MultiScanCountListener;
import com.bazaarvoice.emodb.web.scanner.notifications.MultiStashStateListener;
import com.bazaarvoice.emodb.web.scanner.notifications.SNSStashStateListener;
import com.bazaarvoice.emodb.web.scanner.notifications.ScanCountListener;
import com.bazaarvoice.emodb.web.scanner.rangescan.LocalRangeScanUploader;
import com.bazaarvoice.emodb.web.scanner.rangescan.RangeScanUploader;
import com.bazaarvoice.emodb.web.scanner.scanstatus.DataStoreStashRequestDAO;
import com.bazaarvoice.emodb.web.scanner.scanstatus.DataStoreScanStatusDAO;
import com.bazaarvoice.emodb.web.scanner.scanstatus.StashRequestDAO;
import com.bazaarvoice.emodb.web.scanner.scanstatus.StashRequestTable;
import com.bazaarvoice.emodb.web.scanner.scanstatus.StashRequestTablePlacement;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatusDAO;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatusTable;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatusTablePlacement;
import com.bazaarvoice.emodb.web.scanner.scheduling.ScanParticipationService;
import com.bazaarvoice.emodb.web.scanner.scheduling.StashRequestManager;
import com.bazaarvoice.emodb.web.scanner.scheduling.ScanUploadSchedulingService;
import com.bazaarvoice.emodb.web.scanner.scheduling.ScheduledDailyScanUpload;
import com.bazaarvoice.emodb.web.scanner.writer.AmazonS3Provider;
import com.bazaarvoice.emodb.web.scanner.writer.DiscardingScanWriter;
import com.bazaarvoice.emodb.web.scanner.writer.FileScanWriter;
import com.bazaarvoice.emodb.web.scanner.writer.S3CredentialsProvider;
import com.bazaarvoice.emodb.web.scanner.writer.S3ScanWriter;
import com.bazaarvoice.emodb.web.scanner.writer.ScanWriterFactory;
import com.bazaarvoice.emodb.web.scanner.writer.ScanWriterGenerator;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.bazaarvoice.ostrich.dropwizard.pool.ManagedServicePoolProxy;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.inject.*;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.sun.jersey.api.client.Client;
import io.dropwizard.setup.Environment;
import org.apache.curator.framework.CuratorFramework;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Guice module for use with {@link com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode#SCANNER}
 * <p>
 *
 * Exports the following:
 * <li> {@link ScanUploader}
 * </ul>
 */
public class ScanUploadModule extends PrivateModule {
    private final ScannerConfiguration _config;

    public ScanUploadModule(ScannerConfiguration config) {
        checkArgument(config.getScanThreadCount() > 0, "Scan thread count must be at least 1");
        checkArgument(config.getUploadThreadCount() > 0, "Upload thread count must be at least 1");

        _config = config;
    }

    @Override
    protected void configure() {
        binder().requireExplicitBindings();

        requireBinding(Key.get(String.class, SystemTablePlacement.class));

        bind(ScanUploader.class).asEagerSingleton();
        bind(StashRequestManager.class).asEagerSingleton();
        bind(ScanStatusDAO.class).to(DataStoreScanStatusDAO.class).asEagerSingleton();
        bind(StashRequestDAO.class).to(DataStoreStashRequestDAO.class).asEagerSingleton();
        bind(RangeScanUploader.class).to(LocalRangeScanUploader.class).asEagerSingleton();

        bind(MetricsScanCountListener.class).asEagerSingleton();
        bind(CloudWatchScanCountListener.class);   // Lazily load only if configured

        Class<? extends Provider<? extends ScanWorkflow>> scanWorkflowProvider;
        if (_config.isUseSQSQueues()) {
            scanWorkflowProvider = SQSScanWorkflowProvider.class;
        } else {
            scanWorkflowProvider = QueueScanWorkflowProvider.class;
        }
        bind(ScanWorkflow.class).toProvider(scanWorkflowProvider).asEagerSingleton();

        bind(String.class).annotatedWith(ScanStatusTable.class).toInstance(_config.getScanStatusTable());
        bind(String.class).annotatedWith(StashRequestTable.class).toInstance(_config.getScanRequestTable());
        bind(Integer.class).annotatedWith(MaxConcurrentScans.class).toInstance(_config.getScanThreadCount());

        bind(new TypeLiteral<Optional<String>>(){}).annotatedWith(Names.named("pendingScanRangeQueueName"))
                .toInstance(_config.getPendingScanRangeQueueName());
        bind(new TypeLiteral<Optional<String>>(){}).annotatedWith(Names.named("completeScanRangeQueueName"))
                .toInstance(_config.getCompleteScanRangeQueueName());

        bind(ScanWriterGenerator.class).asEagerSingleton();
        install(new FactoryModuleBuilder()
                .implement(FileScanWriter.class, FileScanWriter.class)
                .implement(S3ScanWriter.class, S3ScanWriter.class)
                .implement(DiscardingScanWriter.class, DiscardingScanWriter.class)
                .build(ScanWriterFactory.class));

        // Monitors active upload status, active only by leader election
        bind(ScanUploadMonitor.class).asEagerSingleton();
        // Monitors for new scans waiting to start
        bind(DistributedScanRangeMonitor.class).asEagerSingleton();
        // Schedules any daily scans
        bind(ScanUploadSchedulingService.class).asEagerSingleton();
        // Sends notification that the service is active at the start of a scheduled scan
        bind(ScanParticipationService.class).asEagerSingleton();

        bind(AWSCredentialsProvider.class).toInstance(new DefaultAWSCredentialsProviderChain());
        bind(AmazonS3Provider.class).asEagerSingleton();

        expose(ScanUploader.class);
        expose(StashRequestManager.class);
    }

    @Provides
    @Singleton
    @ScanStatusTablePlacement
    protected String provideScanStatusTablePlacement(@SystemTablePlacement String tablePlacement) {
        return tablePlacement;
    }

    @Provides
    @Singleton
    @StashRequestTablePlacement
    protected String provideScanRequestTablePlacement(@SystemTablePlacement String tablePlacement) {
        return tablePlacement;
    }
    
    @Provides
    @Singleton
    protected Region provideAmazonRegion() {
        return Objects.firstNonNull(Regions.getCurrentRegion(), Region.getRegion(Regions.US_EAST_1));
    }

    @Provides
    @Singleton
    @S3CredentialsProvider
    protected AWSCredentialsProvider provideAmazonS3CredentialsProvider(AWSCredentialsProvider credentialsProvider,
                                                                        @SelfHostAndPort HostAndPort hostAndPort) {
        AWSCredentialsProvider s3CredentialsProvider = credentialsProvider;
        if (_config.getS3AssumeRole().isPresent()) {
            s3CredentialsProvider = new STSAssumeRoleSessionCredentialsProvider(
                    credentialsProvider, _config.getS3AssumeRole().get(), "stash-" + hostAndPort.getHostText());
        }
        return s3CredentialsProvider;
    }

    @Provides
    @Singleton
    protected AmazonSNS provideAmazonSNS(Region region, AWSCredentialsProvider credentialsProvider) {
        AmazonSNS amazonSNS = new AmazonSNSClient(credentialsProvider);
        amazonSNS.setRegion(region);
        return amazonSNS;
    }

    @Provides
    @Singleton
    protected AmazonSQS provideAmazonSQS(Region region, AWSCredentialsProvider credentialsProvider) {
        AmazonSQS amazonSQS = new AmazonSQSClient(credentialsProvider);
        amazonSQS.setRegion(region);
        return amazonSQS;
    }

    @Provides
    @Singleton
    protected AmazonCloudWatch provideAmazonCloudWatch(Region region, AWSCredentialsProvider credentialsProvider) {
        AmazonCloudWatch cloudWatch = new AmazonCloudWatchClient(credentialsProvider);
        cloudWatch.setRegion(region);
        return cloudWatch;
    }

    @Provides
    @Singleton
    @ScanUploadService
    protected ScheduledExecutorService provideUploadExecutorService(Environment environment) {
        return environment.lifecycle().scheduledExecutorService("ScanUpload-%d")
                .threads(_config.getUploadThreadCount()).build();
    }

    @Provides
    @Singleton
    protected List<ScheduledDailyScanUpload> provideScheduledScanUploads(DataStore dataStore) {
        Map<String, ScheduledScanConfiguration> scheduledScanConfigs = _config.getScheduledScans();
        if (scheduledScanConfigs.isEmpty()) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<ScheduledDailyScanUpload> scheduledScanUploads = ImmutableList.builder();

        for (Map.Entry<String, ScheduledScanConfiguration> entry : scheduledScanConfigs.entrySet()) {
            String id = entry.getKey();
            ScheduledScanConfiguration scheduledScanConfig = entry.getValue();

            if (!scheduledScanConfig.getDailyScanTime().isPresent()) {
                // Not configured to run daily, so skip it
                continue;
            }

            checkArgument(!scheduledScanConfig.getPlacements().isEmpty(), "Scheduled scan must contain at least one placement");
            checkArgument(scheduledScanConfig.getScanId().isPresent(), "Scan ID not set");
            checkArgument(scheduledScanConfig.getMaxRangeConcurrency().isPresent(), "Max range concurrency not set");
            checkArgument(scheduledScanConfig.getScanByAZ().isPresent(), "Scan by availability zone is not set");
            checkArgument(scheduledScanConfig.getRangeScanSplitSize() > 0, "Max range scan split size must be > 0");
            checkArgument(scheduledScanConfig.getMaxRangeScanTime().toStandardDuration().isLongerThan(Duration.ZERO), "Duration must be longer than zero");

            ScanDestination destination = ScanDestination.to(dataStore.getStashRoot());
            DateTimeFormatter scanIdFormatter = DateTimeFormat.forPattern(scheduledScanConfig.getScanId().get()).withZoneUTC();
            DateTimeFormatter dateFormatter;
            if (scheduledScanConfig.getScanDirectory().isPresent()) {
                dateFormatter = DateTimeFormat.forPattern(scheduledScanConfig.getScanDirectory().get()).withZoneUTC();
            } else {
                dateFormatter = StashUtil.STASH_DIRECTORY_DATE_FORMAT;
            }

            scheduledScanUploads.add(new ScheduledDailyScanUpload(
                    id,
                    scheduledScanConfig.getDailyScanTime().get(),
                    scanIdFormatter,
                    destination,
                    dateFormatter,
                    scheduledScanConfig.getPlacements(),
                    scheduledScanConfig.getMaxRangeConcurrency().get(),
                    scheduledScanConfig.getScanByAZ().get(),
                    scheduledScanConfig.isRequestRequired(),
                    scheduledScanConfig.getRangeScanSplitSize(),
                    scheduledScanConfig.getMaxRangeScanTime().toStandardDuration()));
        }

        return scheduledScanUploads.build();
    }

    @Provides
    @Singleton
    protected MetricsStashStateListener provideMetricsStashStateListener(Environment environment, PluginServerMetadata metadata) {
        MetricsStashStateListener listener = new MetricsStashStateListener();
        listener.init(environment, metadata, null);
        return listener;
    }

    @Provides
    @Singleton
    protected Optional<SNSStashStateListener> provideSNSStashStateListener(AmazonSNS amazonSNS, Environment environment,
                                                                           PluginServerMetadata metadata) {
        String snsTopic = _config.getNotifications().getSnsTopic();
        if (snsTopic != null) {
            SNSStashStateListener listener = new SNSStashStateListener(amazonSNS, snsTopic);
            listener.init(environment, metadata, null);
            return Optional.of(listener);
        }
        return Optional.absent();
    }

    @Provides
    @Singleton
    @Named ("plugin")
    protected List<StashStateListener> providePluginStashStateListeners(Environment environment, PluginServerMetadata metadata) {
        List<PluginConfiguration> pluginConfigs = _config.getNotifications().getStashStateListenerPluginConfigurations();
        if (pluginConfigs.isEmpty()) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<StashStateListener> listeners = ImmutableList.builder();
        for (PluginConfiguration config : pluginConfigs) {
            final StashStateListener listener = PluginInstanceGenerator.generateInstance(
                    config.getClassName(), StashStateListener.class, config.getConfig(),
                    environment, metadata);
            listeners.add(listener);
        }

        return listeners.build();
    }

    @Provides
    @Singleton
    protected StashStateListener provideStashStateListener(MetricsStashStateListener metricsListener,
                                                           Optional<SNSStashStateListener> snsListener,
                                                           @Named("plugin") List<StashStateListener> pluginListeners) {
        List<StashStateListener> listeners = Lists.newArrayListWithCapacity(3);
        listeners.add(metricsListener);
        if (snsListener.isPresent()) {
            listeners.add(snsListener.get());
        }
        listeners.addAll(pluginListeners);
        return MultiStashStateListener.combine(listeners);
    }

    @Provides
    @Singleton
    protected List<Dimension> provideCloudWatchDimensions() {
        return FluentIterable.from(_config.getNotifications().getCloudWatchDimensions().entrySet())
                .transform(new Function<Map.Entry<String, String>, Dimension>() {
                    @Override
                    public Dimension apply(Map.Entry<String, String> entry) {
                        return new Dimension()
                                .withName(entry.getKey())
                                .withValue(entry.getValue());
                    }
                })
                .toList();
    }

    @Provides
    @Singleton
    protected Optional<CloudWatchScanCountListener> provideCloudWatchScanListener(
            Provider<CloudWatchScanCountListener> provider) {
        if (_config.getNotifications().isEnableCloudWatchMetrics()) {
            return Optional.of(provider.get());
        }
        return Optional.absent();
    }

    @Provides
    @Singleton
    protected ScanCountListener provideScanCountListener(MetricsScanCountListener metricsListener,
                                                         Optional<CloudWatchScanCountListener> cloudWatchScanCountListener) {
        List<ScanCountListener> listeners = Lists.newArrayListWithCapacity(2);
        listeners.add(metricsListener);
        if (cloudWatchScanCountListener.isPresent()) {
            listeners.add(cloudWatchScanCountListener.get());
        }
        return MultiScanCountListener.combine(listeners);
    }

    @Provides
    @Singleton
    @Named ("ScannerAPIKey")
    protected String provideScannerApiKey(@ServerCluster String cluster) {
        if (!_config.getScannerApiKey().isPresent()) {
            return "anonymous";
        }
        String scannerApiKey = _config.getScannerApiKey().get();
        if (ApiKeyEncryption.isPotentiallyEncryptedApiKey(scannerApiKey)) {
            scannerApiKey = new ApiKeyEncryption(cluster).decrypt(scannerApiKey);
        } else {
            LoggerFactory.getLogger("com.bazaarvoice.emodb.security").warn(
                    "Scanner API key is stored in plaintext; anyone with access to config.yaml can see it!!!");
        }
        return scannerApiKey;
    }

    /** Provider used internally when EmoDB queues are configured */
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
                                         Client client, @Named ("ScannerAPIKey") String apiKey,
                                         @Named ("pendingScanRangeQueueName") Optional<String> pendingScanRangeQueueName,
                                         @Named ("completeScanRangeQueueName") Optional<String> completeScanRangeQueueName,
                                         Environment environment, MetricRegistry metricRegistry) {
            _curator = curator;
            _cluster = cluster;
            _client = client;
            _apiKey = apiKey;
            _environment = environment;
            _metricRegistry = metricRegistry;
            _pendingScanRangeQueueName = pendingScanRangeQueueName.or("emodb-pending-scan-ranges");
            _completeScanRangeQueueName = completeScanRangeQueueName.or("emodb-complete-scan-ranges");
        }

        @Override
        public ScanWorkflow get() {
            QueueClientFactory factory = QueueClientFactory.forClusterAndHttpClient(_cluster, _client);

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

    /** Provider used internally when SQS queues are configured */
    public static class SQSScanWorkflowProvider implements Provider<ScanWorkflow> {
        private final AmazonSQS _amazonSQS;
        private final String _pendingScanRangeQueueName;
        private final String _completeScanRangeQueueName;

        @Inject
        public SQSScanWorkflowProvider(@ServerCluster String cluster, AmazonSQS amazonSQS,
                                       @Named ("pendingScanRangeQueueName") Optional<String> pendingScanRangeQueueName,
                                       @Named ("completeScanRangeQueueName") Optional<String> completeScanRangeQueueName) {
            _amazonSQS = amazonSQS;
            _pendingScanRangeQueueName = pendingScanRangeQueueName.or(String.format("emodb-pending-scan-ranges-%s", cluster));
            _completeScanRangeQueueName = completeScanRangeQueueName.or(String.format("emodb-complete-scan-ranges-%s", cluster));
        }

        @Override
        public ScanWorkflow get() {
            return new SQSScanWorkflow(_amazonSQS, _pendingScanRangeQueueName, _completeScanRangeQueueName);
        }
    }
}
