package com.bazaarvoice.emodb.databus;

import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.common.cassandra.CassandraFactory;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkBooleanSerializer;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkValueStore;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.core.CanaryManager;
import com.bazaarvoice.emodb.databus.core.DatabusChannelConfiguration;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.bazaarvoice.emodb.databus.core.DatabusFactory;
import com.bazaarvoice.emodb.databus.core.DedupMigrationTask;
import com.bazaarvoice.emodb.databus.core.DefaultDatabus;
import com.bazaarvoice.emodb.databus.core.DefaultFanoutManager;
import com.bazaarvoice.emodb.databus.core.DefaultRateLimitedLogFactory;
import com.bazaarvoice.emodb.databus.core.DrainFanoutPartitionTask;
import com.bazaarvoice.emodb.databus.core.FanoutLagMonitor;
import com.bazaarvoice.emodb.databus.core.FanoutManager;
import com.bazaarvoice.emodb.databus.core.HashingPartitionSelector;
import com.bazaarvoice.emodb.databus.core.MasterFanout;
import com.bazaarvoice.emodb.databus.core.OwnerAwareDatabus;
import com.bazaarvoice.emodb.databus.core.PartitionSelector;
import com.bazaarvoice.emodb.databus.core.RateLimitedLogFactory;
import com.bazaarvoice.emodb.databus.core.SubscriptionEvaluator;
import com.bazaarvoice.emodb.databus.core.SystemQueueMonitorManager;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.db.cql.CqlSubscriptionDAO;
import com.bazaarvoice.emodb.databus.db.generic.CachingSubscriptionDAO;
import com.bazaarvoice.emodb.databus.db.generic.CachingSubscriptionDAODelegate;
import com.bazaarvoice.emodb.databus.db.generic.CachingSubscriptionDAOExecutorService;
import com.bazaarvoice.emodb.databus.db.generic.CachingSubscriptionDAORegistry;
import com.bazaarvoice.emodb.databus.kafka.KafkaConsumerConfiguration;
import com.bazaarvoice.emodb.databus.kafka.KafkaProducerConfiguration;
import com.bazaarvoice.emodb.databus.kafka.KafkaTopicConfiguration;
import com.bazaarvoice.emodb.databus.repl.DefaultReplicationManager;
import com.bazaarvoice.emodb.databus.repl.DefaultReplicationSource;
import com.bazaarvoice.emodb.databus.repl.ReplicationEnabledTask;
import com.bazaarvoice.emodb.databus.repl.ReplicationSource;
import com.bazaarvoice.emodb.event.DedupEnabled;
import com.bazaarvoice.emodb.event.EventStoreHostDiscovery;
import com.bazaarvoice.emodb.event.EventStoreModule;
import com.bazaarvoice.emodb.event.EventStoreZooKeeper;
import com.bazaarvoice.emodb.event.api.ChannelConfiguration;
import com.bazaarvoice.emodb.event.api.DedupEventStoreChannels;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.sor.DataStoreConfiguration;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.ostrich.HostDiscovery;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.collect.Range;
import com.google.common.eventbus.EventBus;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.sun.jersey.api.client.Client;
import io.dropwizard.lifecycle.ExecutorServiceManager;
import io.dropwizard.util.Duration;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.curator.framework.CuratorFramework;

import java.time.Clock;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Guice module for constructing a {@link Databus}.
 * <p>
 * Requires the following external references:
 * <ul>
 * <li> {@link DatabusConfiguration}
 * <li> {@link CacheRegistry}
 * <li> {@link HealthCheckRegistry}
 * <li> {@link LeaderServiceTask}
 * <li> {@link LifeCycleRegistry}
 * <li> {@link TaskRegistry}
 * <li> {@link JobHandlerRegistry}
 * <li> {@link JobService}
 * <li> @{@link SelfHostAndPort} {@link HostAndPort}
 * <li> @{@link DatabusHostDiscovery} {@link HostDiscovery}
 * <li> @{@link DatabusZooKeeper} {@link CuratorFramework}
 * <li> @{@link Global} {@link CuratorFramework}
 * <li> Jersey {@link Client}
 * <li> @{@link ReplicationKey} String
 * <li> @{@link SystemIdentity} String
 * <li> DataStore {@link DataProvider}
 * <li> DataStore {@link EventBus}
 * <li> DataStore {@link DataStoreConfiguration}
 * <li> {@link com.bazaarvoice.emodb.databus.auth.DatabusAuthorizer}
 * <li> @{@link DefaultJoinFilter} Supplier&lt;{@link Condition}&gt;
 * <li> {@link Clock}
 * </ul>
 * Exports the following:
 * <ul>
 * <li> {@link DatabusFactory}
 * <li> {@link DatabusEventStore}
 * <li> {@link ReplicationSource}
 * </ul>
 */
public class DatabusModule extends PrivateModule {
    private static final int MAX_THREADS_FOR_QUEUE_DRAINING = 10;

    // TODO this should really be configurable
    private static final int MAX_THREADS_FOR_KAFKA_FANOUT = 1;
    private static final int MAX_THREADS_FOR_KAFKA_RESOLVER = 1;
    private static final int MAX_THREADS_FOR_KAFKA_RESOLVER_RETRY = 1;

    private final EmoServiceMode _serviceMode;
    private MetricRegistry _metricRegistry;

    public DatabusModule(EmoServiceMode serviceMode, MetricRegistry metricRegistry) {
        _serviceMode = serviceMode;
        _metricRegistry = metricRegistry;
    }

    @Override
    protected void configure() {
        // Chain SubscriptionDAO -> CachingSubscriptionDAO -> CqlSubscriptionDAO.
        bind(SubscriptionDAO.class).to(CachingSubscriptionDAO.class).asEagerSingleton();
        bind(SubscriptionDAO.class).annotatedWith(CachingSubscriptionDAODelegate.class).to(CqlSubscriptionDAO.class).asEagerSingleton();
        bind(CqlSubscriptionDAO.class).asEagerSingleton();
        bind(CassandraFactory.class).asEagerSingleton();

        // Event Store
        bind(ChannelConfiguration.class).to(DatabusChannelConfiguration.class).asEagerSingleton();
        bind(CuratorFramework.class).annotatedWith(EventStoreZooKeeper.class).to(Key.get(CuratorFramework.class, DatabusZooKeeper.class));
        bind(HostDiscovery.class).annotatedWith(EventStoreHostDiscovery.class).to(Key.get(HostDiscovery.class, DatabusHostDiscovery.class));
        bind(DedupEventStoreChannels.class).toInstance(ChannelNames.dedupChannels());
        bind(new TypeLiteral<Supplier<Boolean>>() {}).annotatedWith(DedupEnabled.class)
                .to(Key.get(new TypeLiteral<ValueStore<Boolean>>() {}, DedupEnabled.class)).asEagerSingleton();

        install(new EventStoreModule("bv.emodb.databus", _metricRegistry));

        // Databus Fanout and Replication
        if (_serviceMode.specifies(EmoServiceMode.Aspect.dataBus_fan_out_and_replication)) {
            bind(FanoutManager.class).to(DefaultFanoutManager.class).asEagerSingleton();
            bind(CanaryManager.class).asEagerSingleton();
            bind(MasterFanout.class).asEagerSingleton();
            bind(DefaultReplicationManager.class).asEagerSingleton();
            bind(ReplicationEnabledTask.class).asEagerSingleton();
            bind(SystemQueueMonitorManager.class).asEagerSingleton();
            bind(FanoutLagMonitor.class).asEagerSingleton();
        }

        // Databus
        bind(RateLimitedLogFactory.class).to(DefaultRateLimitedLogFactory.class).asEagerSingleton();
        bind(SubscriptionEvaluator.class).asEagerSingleton();
        bind(DedupMigrationTask.class).asEagerSingleton();
        bind(DrainFanoutPartitionTask.class).asEagerSingleton();
        
        // Expose the event store directly for use by debugging APIs
        bind(DatabusEventStore.class).asEagerSingleton();
        expose(DatabusEventStore.class);

        // Bind the Databus instance that the rest of the application will consume
        bind(OwnerAwareDatabus.class).to(DefaultDatabus.class).asEagerSingleton();
        bind(DatabusFactory.class).asEagerSingleton();
        expose(DatabusFactory.class);

        // Bind the cross-data center outbound replication end point
        bind(ReplicationSource.class).to(DefaultReplicationSource.class).asEagerSingleton();
        expose(ReplicationSource.class);

    }

    @Provides @Singleton
    CassandraKeyspace provideKeyspace(DatabusConfiguration configuration, CassandraFactory factory) {
        Map<String, CassandraKeyspace> keyspaces = factory.build(configuration.getCassandraConfiguration());
        // Databus module should only have one keyspace configured
        checkArgument(keyspaces.size() == 1, "Only one keyspace expected for databus, found %s", keyspaces.keySet());
        return keyspaces.values().iterator().next();
    }

    @Provides @Singleton @CachingSubscriptionDAORegistry
    CacheRegistry provideCacheRegistry(CacheRegistry cacheRegistry) {
        return cacheRegistry.withNamespace("bus");
    }

    @Provides @Singleton @DedupEnabled
    ValueStore<Boolean> provideDedupEnabled(@DatabusZooKeeper CuratorFramework curator,
                                            LifeCycleRegistry lifeCycle) {
        return lifeCycle.manage(
                new ZkValueStore<>(curator, "/settings/dedup-enabled", new ZkBooleanSerializer(), true));
    }

    @Provides @Singleton @ReplicationEnabled
    ValueStore<Boolean> provideReplicationEnabled(@DatabusZooKeeper CuratorFramework curator,
                                                  LifeCycleRegistry lifeCycle) {
        return lifeCycle.manage(
                new ZkValueStore<>(curator, "/settings/replication-enabled", new ZkBooleanSerializer(), true));
    }

    @Provides @Singleton @CachingSubscriptionDAOExecutorService
    ListeningExecutorService provideCachingSubscriptionDAOExecutorService(LifeCycleRegistry lifeCycleRegistry) {
        ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(
                1, new ThreadFactoryBuilder().setNameFormat("subscription-cache-%d").build()));
        lifeCycleRegistry.manage(new ExecutorServiceManager(service, Duration.seconds(1), "subscription-cache"));
        return service;
    }

    @Provides @Singleton
    CachingSubscriptionDAO.CachingMode provideCachingSubscriptionDAOCachingMode(DatabusConfiguration configuration) {
        return configuration.getSubscriptionCacheInvalidation();
    }

    @Provides @Singleton @QueueDrainExecutorService
    ExecutorService provideQueueDrainService (LifeCycleRegistry lifeCycleRegistry) {
        ExecutorService queueDrainService = Executors.newFixedThreadPool(MAX_THREADS_FOR_QUEUE_DRAINING, new ThreadFactoryBuilder().setNameFormat("drainQueue-%d").build());
        lifeCycleRegistry.manage(new ExecutorServiceManager(queueDrainService, Duration.seconds(1), "drainQueue-cache"));
        return queueDrainService;
    }

    @Provides @Singleton @MasterFanoutPartitions
    Integer provideMasterFanoutPartitions(DatabusConfiguration configuration) {
        checkArgument(Range.closed(1, 16).contains(configuration.getMasterFanoutPartitions()),
                "Master fanout partitions must be between 1 and 16");
        return configuration.getMasterFanoutPartitions();
    }

    @Provides @Singleton @MasterFanoutPartitions
    PartitionSelector provideMasterFanoutPartitionSelector(@MasterFanoutPartitions int numPartitions) {
        if (numPartitions == 1) {
            return PartitionSelector.SINGLE_PARTITION_SELECTOR;
        }
        return new HashingPartitionSelector(numPartitions);
    }

    @Provides @Singleton @DataCenterFanoutPartitions
    Integer provideDataCenterFanoutPartitions(DatabusConfiguration configuration) {
        checkArgument(Range.closed(1, 16).contains(configuration.getDataCenterFanoutPartitions()),
                "Data center fanout partitions must be between 1 and 16");
        return configuration.getDataCenterFanoutPartitions();
    }

    @Provides @Singleton @DataCenterFanoutPartitions
    PartitionSelector provideDataCenterFanoutPartitionSelector(@DataCenterFanoutPartitions int numPartitions) {
        if (numPartitions == 1) {
            return PartitionSelector.SINGLE_PARTITION_SELECTOR;
        }
        return new HashingPartitionSelector(numPartitions);
    }

    @Provides @Singleton @KafkaEnabled
    Boolean provideKafkaEnabled(DatabusConfiguration configuration) { return configuration.getKafkaEnabled(); }

    @Provides @Singleton @KafkaTestForceRetry
    Boolean provideKafkaTestForceRetry(DatabusConfiguration configuration) { return configuration.getKafkaTestForceRetry(); }

    @Provides @Singleton @KafkaTestForceRetryToFail
    Boolean provideKafkaTestForceRetryToFail(DatabusConfiguration configuration) { return configuration.getKafkaTestForceRetryToFail(); }

    @Provides @Singleton @KafkaEventProducerConfiguration
    KafkaProducerConfiguration provideKafkaEventProducerConfiguration(DatabusConfiguration configuration) {
        return configuration.getEventProducerConfiguration();
    }

    @Provides @Singleton @KafkaResolvedEventProducerConfiguration
    KafkaProducerConfiguration provideKafkaResolvedEventProducerConfiguration(DatabusConfiguration configuration) {
        return configuration.getResolvedEventProducerConfiguration();
    }

    @Provides @Singleton @KafkaMasterQueueTopicConfiguration
    KafkaTopicConfiguration provideKafkaMasterQueueTopicConfiguration(DatabusConfiguration configuration) {
        return configuration.getMasterQueueTopicConfiguration();
    }

    @Provides @Singleton @KafkaResolverRetryQueueTopicConfiguration
    KafkaTopicConfiguration provideKafkaResolverRetryQueueTopicConfiguration(DatabusConfiguration configuration) {
        return configuration.getResolverRetryQueueTopicConfiguration();
    }

}
