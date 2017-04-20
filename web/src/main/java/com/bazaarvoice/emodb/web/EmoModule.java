package com.bazaarvoice.emodb.web;

import com.bazaarvoice.curator.dropwizard.ZooKeeperConfiguration;
import com.bazaarvoice.emodb.auth.AuthCacheRegistry;
import com.bazaarvoice.emodb.auth.AuthZooKeeper;
import com.bazaarvoice.emodb.blob.BlobStoreConfiguration;
import com.bazaarvoice.emodb.blob.BlobStoreModule;
import com.bazaarvoice.emodb.blob.BlobStoreZooKeeper;
import com.bazaarvoice.emodb.cachemgr.CacheManagerModule;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.invalidate.InvalidationService;
import com.bazaarvoice.emodb.common.cassandra.CqlDriverConfiguration;
import com.bazaarvoice.emodb.common.dropwizard.discovery.DropwizardResourceRegistry;
import com.bazaarvoice.emodb.common.dropwizard.discovery.PayloadBuilder;
import com.bazaarvoice.emodb.common.dropwizard.discovery.ResourceRegistry;
import com.bazaarvoice.emodb.common.dropwizard.discovery.ServiceNames;
import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfAdminHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPortModule;
import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.DropwizardHealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.DropwizardLifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.common.dropwizard.task.DropwizardTaskRegistry;
import com.bazaarvoice.emodb.common.dropwizard.task.IgnoreAllTaskRegistry;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkMapStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkTimestampSerializer;
import com.bazaarvoice.emodb.databus.DatabusConfiguration;
import com.bazaarvoice.emodb.databus.DatabusHostDiscovery;
import com.bazaarvoice.emodb.databus.DatabusModule;
import com.bazaarvoice.emodb.databus.DatabusZooKeeper;
import com.bazaarvoice.emodb.databus.DefaultJoinFilter;
import com.bazaarvoice.emodb.databus.SystemIdentity;
import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.auth.DatabusAuthorizer;
import com.bazaarvoice.emodb.databus.auth.FilteredDatabusAuthorizer;
import com.bazaarvoice.emodb.databus.auth.SystemProcessDatabusAuthorizer;
import com.bazaarvoice.emodb.databus.core.DatabusFactory;
import com.bazaarvoice.emodb.datacenter.DataCenterConfiguration;
import com.bazaarvoice.emodb.datacenter.DataCenterModule;
import com.bazaarvoice.emodb.job.JobConfiguration;
import com.bazaarvoice.emodb.job.JobModule;
import com.bazaarvoice.emodb.job.JobZooKeeper;
import com.bazaarvoice.emodb.plugin.PluginConfiguration;
import com.bazaarvoice.emodb.plugin.PluginServerMetadata;
import com.bazaarvoice.emodb.plugin.lifecycle.ServerStartedListener;
import com.bazaarvoice.emodb.plugin.util.PluginInstanceGenerator;
import com.bazaarvoice.emodb.queue.DedupQueueHostDiscovery;
import com.bazaarvoice.emodb.queue.QueueConfiguration;
import com.bazaarvoice.emodb.queue.QueueModule;
import com.bazaarvoice.emodb.queue.QueueZooKeeper;
import com.bazaarvoice.emodb.queue.api.AuthDedupQueueService;
import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.queue.api.DedupQueueService;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.emodb.queue.client.DedupQueueClientFactory;
import com.bazaarvoice.emodb.queue.client.DedupQueueServiceAuthenticator;
import com.bazaarvoice.emodb.queue.client.QueueClientFactory;
import com.bazaarvoice.emodb.queue.client.QueueServiceAuthenticator;
import com.bazaarvoice.emodb.queue.core.TrustedDedupQueueService;
import com.bazaarvoice.emodb.queue.core.TrustedQueueService;
import com.bazaarvoice.emodb.sor.DataStoreConfiguration;
import com.bazaarvoice.emodb.sor.DataStoreModule;
import com.bazaarvoice.emodb.sor.DataStoreZooKeeper;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.client.DataStoreClient;
import com.bazaarvoice.emodb.sor.client.DataStoreClientFactory;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DataStoreAsyncModule;
import com.bazaarvoice.emodb.sor.core.SystemDataStore;
import com.bazaarvoice.emodb.sor.db.cql.CqlForMultiGets;
import com.bazaarvoice.emodb.sor.db.cql.CqlForScans;
import com.bazaarvoice.emodb.table.db.consistency.GlobalFullConsistencyZooKeeper;
import com.bazaarvoice.emodb.web.auth.AuthorizationConfiguration;
import com.bazaarvoice.emodb.web.auth.OwnerDatabusAuthorizer;
import com.bazaarvoice.emodb.web.auth.SecurityModule;
import com.bazaarvoice.emodb.web.partition.PartitionAwareClient;
import com.bazaarvoice.emodb.web.partition.PartitionAwareServiceFactory;
import com.bazaarvoice.emodb.web.plugins.DefaultPluginServerMetadata;
import com.bazaarvoice.emodb.web.report.ReportsModule;
import com.bazaarvoice.emodb.web.resources.blob.ApprovedBlobContentTypes;
import com.bazaarvoice.emodb.web.resources.databus.DatabusRelayClientFactory;
import com.bazaarvoice.emodb.web.resources.databus.DatabusResourcePoller;
import com.bazaarvoice.emodb.web.resources.databus.LocalSubjectDatabus;
import com.bazaarvoice.emodb.web.resources.databus.LongPollingExecutorServices;
import com.bazaarvoice.emodb.web.resources.databus.SubjectDatabus;
import com.bazaarvoice.emodb.web.resources.databus.SubjectDatabusClientFactory;
import com.bazaarvoice.emodb.web.scanner.ScanUploadModule;
import com.bazaarvoice.emodb.web.scanner.ScannerZooKeeper;
import com.bazaarvoice.emodb.web.settings.DatabusDefaultJoinFilterConditionAdminTask;
import com.bazaarvoice.emodb.web.settings.Setting;
import com.bazaarvoice.emodb.web.settings.SettingsModule;
import com.bazaarvoice.emodb.web.settings.SettingsRegistry;
import com.bazaarvoice.emodb.web.settings.SorCqlDriverTask;
import com.bazaarvoice.emodb.web.throttling.AdHocThrottle;
import com.bazaarvoice.emodb.web.throttling.AdHocThrottleControlTask;
import com.bazaarvoice.emodb.web.throttling.AdHocThrottleManager;
import com.bazaarvoice.emodb.web.throttling.AdHocThrottleMapStore;
import com.bazaarvoice.emodb.web.throttling.BlackListIpValueStore;
import com.bazaarvoice.emodb.web.throttling.IpBlacklistControlTask;
import com.bazaarvoice.emodb.web.throttling.ZkAdHocThrottleSerializer;
import com.bazaarvoice.emodb.web.util.ZKNamespaces;
import com.bazaarvoice.ostrich.HostDiscovery;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.bazaarvoice.ostrich.ServiceFactory;
import com.bazaarvoice.ostrich.ServiceRegistry;
import com.bazaarvoice.ostrich.discovery.FixedHostDiscovery;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.bazaarvoice.ostrich.dropwizard.pool.ManagedServicePoolProxy;
import com.bazaarvoice.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.registry.zookeeper.ZooKeeperServiceRegistry;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.sun.jersey.api.client.Client;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.setup.Environment;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.blackList;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.blobStore_module;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.cache;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.dataBus_module;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.dataCenter;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.dataStore_module;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.dataStore_web;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.full_consistency;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.job;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.leader_control;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.queue_module;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.report;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.scanner;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.security;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.throttle;
import static com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode.Aspect.web;

public class EmoModule extends AbstractModule {
    private static final Logger _log = LoggerFactory.getLogger(EmoModule.class);
    private final Environment _environment;
    private final EmoConfiguration _configuration;
    private final EmoServiceMode _serviceMode;

    public EmoModule(EmoConfiguration configuration, Environment environment, EmoServiceMode serviceMode) {
        _configuration = configuration;
        _environment = environment;
        _serviceMode = serviceMode;
    }

    @Override
    protected void configure() {
        // Prevent accidents.  All bindings must be explicit.
        binder().requireExplicitBindings();

        // Install modules based on aspects associated with selected service mode
        install(new CommonModuleSetup());
        install(new PluginsSetup());
        evaluate(web, new WebSetup());
        evaluate(cache, new CacheSetup());
        evaluate(dataCenter, new DataCenterSetup());
        evaluate(dataStore_module, new DataStoreSetup());
        evaluate(blobStore_module, new BlobStoreSetup());
        evaluate(dataBus_module, new DatabusSetup());
        evaluate(queue_module, new QueueSetup());
        evaluate(leader_control, new LeaderControlSetup());
        evaluate(throttle, new ThrottleSetup());
        evaluate(blackList, new BlacklistSetup());
        evaluate(scanner, new ScannerSetup());
        evaluate(report, new ReportSetup());
        evaluate(job, new JobSetup());
        evaluate(security, new SecuritySetup());
        evaluate(full_consistency, new FullConsistencySetup());
        evaluate(dataStore_web, new DataStoreAsyncSetup());
    }

    private class CommonModuleSetup extends AbstractModule {

        @Override
        protected void configure() {
            install(new SettingsModule());
            bind(Environment.class).toInstance(_environment);
            bind(HealthCheckRegistry.class).to(DropwizardHealthCheckRegistry.class).asEagerSingleton();
            bind(LifeCycleRegistry.class).to(DropwizardLifeCycleRegistry.class).asEagerSingleton();
            bind(ZooKeeperConfiguration.class).toInstance(_configuration.getZooKeeperConfiguration());
            bind(String.class).annotatedWith(ServerCluster.class).toInstance(_configuration.getCluster());
            bind(MetricRegistry.class).toInstance(_environment.metrics());
            bind(ServerFactory.class).toInstance(_configuration.getServerFactory());
            bind(DataCenterConfiguration.class).toInstance(_configuration.getDataCenterConfiguration());
            bind(CqlDriverConfiguration.class).toInstance(_configuration.getCqlDriverConfiguration());
            bind(Clock.class).toInstance(Clock.systemUTC());
        }

        /** Connect to ZooKeeper. */
        @Provides @Singleton @Global
        CuratorFramework provideCuratorFramework(ZooKeeperConfiguration configuration, Environment environment) {
            CuratorFramework curator = configuration.newManagedCurator(environment.lifecycle());
            curator.start();
            return curator;
        }

        /** Provide ZooKeeper-based SOA service registry. */
        @Provides @Singleton
        ServiceRegistry provideServiceRegistry(@Global CuratorFramework curator, LifeCycleRegistry lifeCycle, MetricRegistry metricRegistry) {
            return lifeCycle.manage(new ZooKeeperServiceRegistry(curator, metricRegistry));
        }

        /** Provides a ZooKeeper-based set of ad-hoc endpoint throttles. */
        @Provides @Singleton @AdHocThrottleMapStore
        MapStore<AdHocThrottle> provideAdHocThrottleMapStore(@Global CuratorFramework curator, LifeCycleRegistry lifeCycle) {
            CuratorFramework webCurator = withComponentNamespace(curator, "web");
            return lifeCycle.manage(new ZkMapStore<>(webCurator, "/adhoc-throttles", new ZkAdHocThrottleSerializer()));
        }
    }

    private class WebSetup extends AbstractModule {

        @Override
        protected void configure() {
            bind(TaskRegistry.class).to(getTaskRegistryClass()).asEagerSingleton();
            bind(ResourceRegistry.class).to(DropwizardResourceRegistry.class).asEagerSingleton();
            bind(String.class).annotatedWith(ServerCluster.class).toInstance(_configuration.getCluster());
            bind(JerseyClientConfiguration.class).toInstance(_configuration.getHttpClientConfiguration());
            install(new SelfHostAndPortModule());
        }

        /** Configure the HTTP client for out-bound HTTP calls. */
        @Provides @Singleton
        Client provideJerseyClient(JerseyClientConfiguration configuration, Environment environment) {
            return new JerseyClientBuilder(environment).using(configuration).using(environment).build("emodb");
        }

        private Class<? extends TaskRegistry> getTaskRegistryClass() {
            if (_serviceMode.specifies(EmoServiceMode.Aspect.task)) {
                return DropwizardTaskRegistry.class;
            }
            return IgnoreAllTaskRegistry.class;
        }
    }

    private class DataCenterSetup extends AbstractModule {
        @Override
        protected void configure() {
            install(new DataCenterModule(_serviceMode));
        }
    }

    private class CacheSetup extends AbstractModule  {
        @Override
        protected void configure() {
            bind(String.class).annotatedWith(InvalidationService.class).toInstance(
                    ServiceNames.forNamespaceAndBaseServiceName(_configuration.getCluster(), "emodb-cachemgr"));
            install(new CacheManagerModule());
        }
    }

    private class DataStoreSetup extends AbstractModule  {
        @Override
        protected void configure() {
            bind(DataStoreConfiguration.class).toInstance(_configuration.getDataStoreConfiguration());
            bind(new TypeLiteral<Supplier<Boolean>>(){}).annotatedWith(CqlForMultiGets.class)
                    .to(Key.get(new TypeLiteral<Setting<Boolean>>(){}, CqlForMultiGets.class));
            bind(new TypeLiteral<Supplier<Boolean>>(){}).annotatedWith(CqlForScans.class)
                    .to(Key.get(new TypeLiteral<Setting<Boolean>>(){}, CqlForScans.class));
            bind(SorCqlDriverTask.class).asEagerSingleton();

            install(new DataStoreModule(_serviceMode));
        }

        /** Provide ZooKeeper namespaced to SoR data. */
        @Provides @Singleton @DataStoreZooKeeper
        CuratorFramework provideDataStoreZooKeeperConnection(@Global CuratorFramework curator) {
            return withComponentNamespace(curator, "sor");
        }

        /** Provides a DataStore client that delegates to the remote system center data store. */
        @Provides @Singleton @SystemDataStore
        DataStore provideSystemDataStore (DataCenterConfiguration config, Client jerseyClient, @Named ("AdminKey") String apiKey, MetricRegistry metricRegistry) {

            ServiceFactory<DataStore> clientFactory = DataStoreClientFactory
                    .forClusterAndHttpClient(_configuration.getCluster(), jerseyClient)
                    .usingCredentials(apiKey);

            URI uri = config.getSystemDataCenterServiceUri();
            ServiceEndPoint endPoint = new ServiceEndPointBuilder()
                    .withServiceName(clientFactory.getServiceName())
                    .withId(config.getSystemDataCenter())
                    .withPayload(new PayloadBuilder()
                            .withUrl(uri.resolve(DataStoreClient.SERVICE_PATH))
                            .withAdminUrl(uri)
                            .toString())
                    .build();

            return ServicePoolBuilder.create(DataStore.class)
                    .withMetricRegistry(metricRegistry)
                    .withHostDiscovery(new FixedHostDiscovery(endPoint))
                    .withServiceFactory(clientFactory)
                    .buildProxy(new ExponentialBackoffRetry(30, 1, 10, TimeUnit.SECONDS));
        }

        @Provides @Singleton @CqlForMultiGets
        Setting<Boolean> provideUseCqlForMultiGetSetting(SettingsRegistry settingsRegistry) {
            return settingsRegistry.register("sor.cassandra.cql.useCqlForMultiGets", Boolean.class, true);
        }

        @Provides @Singleton @CqlForScans
        Setting<Boolean> provideUseCqlForScansSetting(SettingsRegistry settingsRegistry) {
            return settingsRegistry.register("sor.cassandra.cql.useCqlForScans", Boolean.class, true);
        }
    }

    private class BlobStoreSetup extends AbstractModule  {
        @Override
        protected void configure() {
            bind(BlobStoreConfiguration.class).toInstance(_configuration.getBlobStoreConfiguration());
            install(new BlobStoreModule(_serviceMode, "bv.emodb.blob", _environment.metrics()));
        }

        @Provides @ApprovedBlobContentTypes
        Set<String> provideApprovedBlobContentTypes(BlobStoreConfiguration config) {
            return config.getApprovedContentTypes();
        }

        /** Provide ZooKeeper namespaced to BlobStore data. */
        @Provides @Singleton @BlobStoreZooKeeper
        CuratorFramework provideBlobStoreZooKeeperConnection(@Global CuratorFramework curator) {
            return withComponentNamespace(curator, "blob");
        }
    }

    private class DatabusSetup extends AbstractModule  {
        @Override
        protected void configure() {
            bind(DatabusConfiguration.class).toInstance(_configuration.getDatabusConfiguration());
            // Used by the databus resource to support long polling
            bind(DatabusResourcePoller.class).asEagerSingleton();
            bind(OwnerDatabusAuthorizer.class).asEagerSingleton();

            // Bind the suppressed event condition setting as the supplier
            bind(new TypeLiteral<Supplier<Condition>>(){}).annotatedWith(DefaultJoinFilter.class)
                    .to(Key.get(new TypeLiteral<Setting<Condition>>(){}, DefaultJoinFilter.class));
            bind(DatabusDefaultJoinFilterConditionAdminTask.class).asEagerSingleton();

            install(new DatabusModule(_serviceMode, _environment.metrics()));
        }

        /** Provides the executor services used for performing long polling, or absent if long polling is disabled. */
        @Provides
        @Singleton
        Optional<LongPollingExecutorServices> provideLongPollingExecutorServices(DatabusConfiguration databusConfiguration) {
            int numPollingThreads = databusConfiguration.getLongPollPollingThreadCount().or(DatabusResourcePoller.DEFAULT_NUM_POLLING_THREADS);
            if (numPollingThreads == 0) {
                return Optional.absent();
            }
            ScheduledExecutorService pollerService = _environment.lifecycle()
                    .scheduledExecutorService("databus-poll-poller-%d")
                    .threads(numPollingThreads)
                    .build();
            numPollingThreads = databusConfiguration.getLongPollKeepAliveThreadCount().or(DatabusResourcePoller.DEFAULT_NUM_KEEP_ALIVE_THREADS);
            ScheduledExecutorService keepAliveService = _environment.lifecycle()
                    .scheduledExecutorService("databus-poll-keepAlive-%d")
                    .threads(numPollingThreads)
                    .build();
            return Optional.of(new LongPollingExecutorServices(pollerService, keepAliveService));
        }

        @Provides @Singleton
        MultiThreadedServiceFactory<AuthDatabus> provideDatabusServiceFactory(Client jerseyClient) {
            // We provide a "DatabusRelayClient" here instead of the standard implementation since our unique use-case
            // (forwarding requests to other nodes) requires some special handling - most notably, we disable long-polling
            // in the poll() requests we forward.
            return DatabusRelayClientFactory.forClusterAndHttpClient(_configuration.getCluster(), jerseyClient);
        }

        @Provides @Singleton
        MultiThreadedServiceFactory<SubjectDatabus> provideSubjectDatabusServiceFactory(
                MultiThreadedServiceFactory<AuthDatabus> authDatabusServiceFactory) {
            // Proxy the AuthDatabus service factory into another service factory that will authorize the caller
            // indirectly using a Subject's API key.
            return new SubjectDatabusClientFactory(authDatabusServiceFactory);
        }

        @Provides @Singleton @DatabusHostDiscovery
        HostDiscovery provideSubjectDatabusHostDiscovery(MultiThreadedServiceFactory<SubjectDatabus> serviceFactory,
                                                  @Global CuratorFramework curator, LifeCycleRegistry lifeCycle) {
            return lifeCycle.manage(new ZooKeeperHostDiscovery(curator, serviceFactory.getServiceName(), _environment.metrics()));
        }

        @Provides @Singleton
        SubjectDatabus provideLocalSubjectDatabus(DatabusFactory databusFactory) {
            return new LocalSubjectDatabus(databusFactory);
        }

        /** Create an SOA Databus client for forwarding non-partition-aware clients to the right server. */
        @Provides @Singleton @PartitionAwareClient
        SubjectDatabus provideSubjectDatabusClient(MultiThreadedServiceFactory<SubjectDatabus> serviceFactory,
                                                   @DatabusHostDiscovery HostDiscovery hostDiscovery,
                                                   SubjectDatabus localSubjectDatabus, @SelfHostAndPort HostAndPort self,
                                                   MetricRegistry metricRegistry, HealthCheckRegistry healthCheckRegistry) {
            SubjectDatabus client = ServicePoolBuilder.create(SubjectDatabus.class)
                    .withHostDiscovery(hostDiscovery)
                    .withServiceFactory(
                            new PartitionAwareServiceFactory<>(SubjectDatabus.class, serviceFactory, localSubjectDatabus, self, healthCheckRegistry))
                    .withMetricRegistry(metricRegistry)
                    .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                    .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
            _environment.lifecycle().manage(new ManagedServicePoolProxy(client));
            return client;
        }

        /** Provide ZooKeeper namespaced to Databus data. */
        @Provides @Singleton @DatabusZooKeeper
        CuratorFramework provideDatabusZooKeeperConnection(@Global CuratorFramework curator) {
            return withComponentNamespace(curator, "bus");
        }

        @Provides @Singleton
        DatabusAuthorizer provideDatabusAuthorizer(OwnerDatabusAuthorizer ownerDatabusAuthorizer,
                                                   @SystemIdentity String systemId) {
            return FilteredDatabusAuthorizer.builder()
                    .withDefaultAuthorizer(ownerDatabusAuthorizer)
                    .withAuthorizerForOwner(systemId, new SystemProcessDatabusAuthorizer(systemId))
                    .build();
        }

        @Provides @Singleton @DefaultJoinFilter
        Setting<Condition> provideDefaultJoinFilterConditionSupplier(SettingsRegistry settingsRegistry) {
            return settingsRegistry.register("databus.defaultJoinFilterCondition", Condition.class, Conditions.alwaysTrue());
        }
    }

    private class QueueSetup extends AbstractModule  {
        @Override
        protected void configure() {
            bind(QueueConfiguration.class).toInstance(_configuration.getQueueConfiguration());
            install(new QueueModule(_environment.metrics()));
        }

        /** Create an SOA QueueService client for forwarding non-partition-aware clients to the right server. */
        @Provides @Singleton @PartitionAwareClient
        QueueServiceAuthenticator provideQueueClient(QueueService queueService, Client jerseyClient,
                                                     @SelfHostAndPort HostAndPort self, @Global CuratorFramework curator,
                                                     MetricRegistry metricRegistry, HealthCheckRegistry healthCheckRegistry) {
            MultiThreadedServiceFactory<AuthQueueService> serviceFactory = new PartitionAwareServiceFactory<>(
                    AuthQueueService.class,
                    QueueClientFactory.forClusterAndHttpClient(_configuration.getCluster(), jerseyClient),
                    new TrustedQueueService(queueService), self, healthCheckRegistry);
            AuthQueueService client = ServicePoolBuilder.create(AuthQueueService.class)
                    .withHostDiscovery(new ZooKeeperHostDiscovery(curator, serviceFactory.getServiceName(), metricRegistry))
                    .withServiceFactory(serviceFactory)
                    .withMetricRegistry(metricRegistry)
                    .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                    .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
            _environment.lifecycle().manage(new ManagedServicePoolProxy(client));
            return QueueServiceAuthenticator.proxied(client);
        }

        /** Provide ZooKeeper namespaced to Queue Service data. */
        @Provides @Singleton @QueueZooKeeper
        CuratorFramework provideQueueZooKeeperConnection(@Global CuratorFramework curator) {
            return withComponentNamespace(curator, "queue");
        }

        @Provides @Singleton
        MultiThreadedServiceFactory<AuthDedupQueueService> provideDedupQueueServiceFactory(Client jerseyClient) {
            return DedupQueueClientFactory.forClusterAndHttpClient(_configuration.getCluster(), jerseyClient);
        }

        @Provides @Singleton @DedupQueueHostDiscovery
        HostDiscovery provideDedupQueueHostDiscovery(MultiThreadedServiceFactory<AuthDedupQueueService> serviceFactory,
                                                     @Global CuratorFramework curator, LifeCycleRegistry lifeCycle) {
            return lifeCycle.manage(new ZooKeeperHostDiscovery(curator, serviceFactory.getServiceName(), _environment.metrics()));
        }

        /** Create an SOA DedupQueue client for forwarding non-partition-aware clients to the right server. */
        @Provides @Singleton @PartitionAwareClient
        DedupQueueServiceAuthenticator provideDedupQueueClient(MultiThreadedServiceFactory<AuthDedupQueueService> serviceFactory,
                                                               @DedupQueueHostDiscovery HostDiscovery hostDiscovery,
                                                               DedupQueueService databus, @SelfHostAndPort HostAndPort self, HealthCheckRegistry healthCheckRegistry) {
            AuthDedupQueueService client = ServicePoolBuilder.create(AuthDedupQueueService.class)
                    .withHostDiscovery(hostDiscovery)
                    .withServiceFactory(new PartitionAwareServiceFactory<>(
                            AuthDedupQueueService.class, serviceFactory, new TrustedDedupQueueService(databus), self, healthCheckRegistry))
                    .withMetricRegistry(_environment.metrics())
                    .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                    .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
            _environment.lifecycle().manage(new ManagedServicePoolProxy(client));
            return DedupQueueServiceAuthenticator.proxied(client);
        }
    }

    private class DataStoreAsyncSetup extends AbstractModule {
        @Override
        protected void configure() {
            install(new DataStoreAsyncModule());
        }
    }

    private class ReportSetup extends AbstractModule  {
        @Override
        protected void configure() {
            install(new ReportsModule());
        }
    }

    private class JobSetup extends AbstractModule  {
        @Override
        protected void configure() {
            bind(JobConfiguration.class).toInstance(_configuration.getJobConfiguration());
            install(new JobModule(_serviceMode));
        }

        /** Provide ZooKeeper namespaced to Job Service data. */
        @Provides @Singleton @JobZooKeeper
        CuratorFramework provideJobZooKeeperConnection(@Global CuratorFramework curator) {
            return withComponentNamespace(curator, "job");
        }
    }

    private class SecuritySetup extends AbstractModule  {
        @Override
        protected void configure() {
            bind(AuthorizationConfiguration.class).toInstance(_configuration.getAuthorizationConfiguration());
            install(new SecurityModule());
        }

        @Provides @Singleton @AuthCacheRegistry
        CacheRegistry provideCacheRegistry(CacheRegistry cacheRegistry) {
            return cacheRegistry.withNamespace("auth");
        }

        @Provides @Singleton @AuthZooKeeper
        CuratorFramework provideAuthZooKeeperConnection(@Global CuratorFramework curator) {
            return withComponentNamespace(curator, "auth");
        }
    }

    private class FullConsistencySetup extends AbstractModule  {
        @Override
        protected void configure() {
        }
        /**
         * Provide ZooKeeper namespaced to GlobalFullConsistencyTimeStamp data.
         * This is to be used by both DataStore and BlobStore for their respective C* Cluster FCT
         */
        @Provides @Singleton @GlobalFullConsistencyZooKeeper
        CuratorFramework provideGlobalFullConsistencyZooKeeperConnection(@Global CuratorFramework curator) {
            return withComponentNamespace(curator, "fct");
        }
    }

    private class BlacklistSetup extends AbstractModule  {
        @Override
        protected void configure() {}

        /** Provides a ZooKeeper-based IP black list. */
        @Provides @Singleton @BlackListIpValueStore
        MapStore<Long> provideBlackListIps(@Global CuratorFramework curator, LifeCycleRegistry lifeCycle) {
            CuratorFramework webCurator = withComponentNamespace(curator, "web");
            return lifeCycle.manage(new ZkMapStore<>(webCurator, "/blacklist", new ZkTimestampSerializer()));
        }
    }

    private class ScannerSetup extends AbstractModule  {
        @Override
        protected void configure() {
            install(new ScanUploadModule(_configuration));
        }

        /** Provide ZooKeeper namespaced to scanner data. */
        @Provides @Singleton @ScannerZooKeeper
        CuratorFramework provideScannerZooKeeperConnection(@Global CuratorFramework curator) {
            return withComponentNamespace(curator, "scanner");
        }
    }

    private class LeaderControlSetup extends AbstractModule  {
        @Override
        protected void configure() {
            bind(LeaderServiceTask.class).asEagerSingleton();
        }
    }

    private class ThrottleSetup extends AbstractModule  {
        @Override
        protected void configure() {
            bind(IpBlacklistControlTask.class).asEagerSingleton();
            bind(AdHocThrottleControlTask.class).asEagerSingleton();
            bind(AdHocThrottleManager.class).asEagerSingleton();
        }
    }

    private class PluginsSetup extends AbstractModule {
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        PluginServerMetadata provicePluginServerMetadata(@ServerCluster String cluster,
                                                         @SelfHostAndPort HostAndPort hostAndPort,
                                                         @SelfAdminHostAndPort HostAndPort adminHostAndPort,
                                                         @Global CuratorFramework curator) {
            return new DefaultPluginServerMetadata(_serviceMode, cluster, hostAndPort, adminHostAndPort,
                    getClass().getPackage().getImplementationVersion(), curator);
        }

        @Provides @Singleton
        List<ServerStartedListener> provideServiceStartedListenerPlugins(PluginServerMetadata pluginServerMetadata) {
            List<PluginConfiguration> pluginConfigs = _configuration.getServerStartedListenerPluginConfigurations();
            if (pluginConfigs.isEmpty()) {
                return ImmutableList.of();
            }

            ImmutableList.Builder<ServerStartedListener> listeners = ImmutableList.builder();
            for (PluginConfiguration config : pluginConfigs) {
                final ServerStartedListener listener = PluginInstanceGenerator.generateInstance(
                        config.getClassName(), ServerStartedListener.class, config.getConfig(),
                        _environment, pluginServerMetadata);
                listeners.add(listener);
            }

            return listeners.build();
        }
    }

    private void evaluate (EmoServiceMode.Aspect aspect, AbstractModule module)  {
        if (_serviceMode.specifies(aspect)) {
            _log.info("running {}", aspect);
            install (module);
        }
    }

    private CuratorFramework withComponentNamespace(CuratorFramework curator, String component) {
        return ZKNamespaces.usingChildNamespace(curator, "applications/emodb", _configuration.getCluster(), component);
    }

}
