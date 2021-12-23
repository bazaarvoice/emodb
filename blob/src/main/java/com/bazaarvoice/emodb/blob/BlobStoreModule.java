package com.bazaarvoice.emodb.blob;

import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.core.BlobStoreProviderProxy;
import com.bazaarvoice.emodb.blob.core.DefaultBlobStore;
import com.bazaarvoice.emodb.blob.core.LocalBlobStore;
import com.bazaarvoice.emodb.blob.core.SystemBlobStore;
import com.bazaarvoice.emodb.blob.db.MetadataProvider;
import com.bazaarvoice.emodb.blob.db.StorageProvider;
import com.bazaarvoice.emodb.blob.db.astyanax.AstyanaxStorageProvider;
import com.bazaarvoice.emodb.blob.db.astyanax.BlobPlacementFactory;
import com.bazaarvoice.emodb.blob.db.s3.AmazonS3Provider;
import com.bazaarvoice.emodb.blob.db.s3.S3HealthCheck;
import com.bazaarvoice.emodb.blob.db.s3.config.S3Configuration;
import com.bazaarvoice.emodb.blob.db.s3.config.S3HealthCheckConfiguration;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.common.cassandra.CassandraFactory;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.cassandra.cqldriver.HintsPollerCQLSession;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.guice.SystemTablePlacement;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.metrics.ParameterizedTimedListener;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkBooleanSerializer;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkDoubleSerializer;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkDurationSerializer;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkMapStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkTimestampSerializer;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkValueStore;
import com.bazaarvoice.emodb.datacenter.DataCenterConfiguration;
import com.bazaarvoice.emodb.datacenter.api.KeyspaceDiscovery;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.table.db.ClusterInfo;
import com.bazaarvoice.emodb.table.db.ShardsPerTable;
import com.bazaarvoice.emodb.table.db.TableChangesEnabled;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxKeyspaceDiscovery;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxTableDAO;
import com.bazaarvoice.emodb.table.db.astyanax.BootstrapTables;
import com.bazaarvoice.emodb.table.db.astyanax.CQLSessionForHintsPollerMap;
import com.bazaarvoice.emodb.table.db.astyanax.CurrentDataCenter;
import com.bazaarvoice.emodb.table.db.astyanax.DataCopyDAO;
import com.bazaarvoice.emodb.table.db.astyanax.DataPurgeDAO;
import com.bazaarvoice.emodb.table.db.astyanax.FullConsistencyTimeProvider;
import com.bazaarvoice.emodb.table.db.astyanax.KeyspaceMap;
import com.bazaarvoice.emodb.table.db.astyanax.Maintenance;
import com.bazaarvoice.emodb.table.db.astyanax.MaintenanceDAO;
import com.bazaarvoice.emodb.table.db.astyanax.MaintenanceRateLimitTask;
import com.bazaarvoice.emodb.table.db.astyanax.MaintenanceSchedulerManager;
import com.bazaarvoice.emodb.table.db.astyanax.MoveTableTask;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementFactory;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementsUnderMove;
import com.bazaarvoice.emodb.table.db.astyanax.RateLimiterCache;
import com.bazaarvoice.emodb.table.db.astyanax.SystemTableNamespace;
import com.bazaarvoice.emodb.table.db.astyanax.TableChangesEnabledTask;
import com.bazaarvoice.emodb.table.db.astyanax.ValidTablePlacements;
import com.bazaarvoice.emodb.table.db.consistency.CassandraClusters;
import com.bazaarvoice.emodb.table.db.consistency.ClusterHintsPoller;
import com.bazaarvoice.emodb.table.db.consistency.CompositeConsistencyTimeProvider;
import com.bazaarvoice.emodb.table.db.consistency.GlobalFullConsistencyZooKeeper;
import com.bazaarvoice.emodb.table.db.consistency.HintsConsistencyTimeProvider;
import com.bazaarvoice.emodb.table.db.consistency.HintsConsistencyTimeTask;
import com.bazaarvoice.emodb.table.db.consistency.HintsConsistencyTimeValues;
import com.bazaarvoice.emodb.table.db.consistency.HintsPollerManager;
import com.bazaarvoice.emodb.table.db.consistency.MinLagConsistencyTimeProvider;
import com.bazaarvoice.emodb.table.db.consistency.MinLagDurationTask;
import com.bazaarvoice.emodb.table.db.consistency.MinLagDurationValues;
import com.bazaarvoice.emodb.table.db.curator.TableMutexManager;
import com.bazaarvoice.emodb.table.db.generic.CachingTableDAO;
import com.bazaarvoice.emodb.table.db.generic.CachingTableDAODelegate;
import com.bazaarvoice.emodb.table.db.generic.CachingTableDAORegistry;
import com.bazaarvoice.emodb.table.db.generic.MutexTableDAO;
import com.bazaarvoice.emodb.table.db.generic.MutexTableDAODelegate;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import com.netflix.astyanax.model.ConsistencyLevel;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Guice module for constructing a {@link BlobStore}.
 * <p>
 * Requires the following external references:
 * <ul>
 * <li> {@link BlobStoreConfiguration}
 * <li> {@link DataCenterConfiguration}
 * <li> {@link CacheRegistry}
 * <li> {@link DataStore}
 * <li> @{@link SystemBlobStore} {@link BlobStore}
 * <li> {@link HealthCheckRegistry}
 * <li> {@link LeaderServiceTask}
 * <li> {@link LifeCycleRegistry}
 * <li> {@link TaskRegistry}
 * <li> @{@link SelfHostAndPort} {@link HostAndPort}
 * <li> Curator {@link CuratorFramework}
 * </ul>
 * Exports the following:
 * <ul>
 * <li> {@link BlobStore}
 * </ul>
 */
public class BlobStoreModule extends PrivateModule {
    private final EmoServiceMode _serviceMode;
    private final String _metricsGroup;
    private final MetricRegistry _metricRegistry;

    public BlobStoreModule(EmoServiceMode serviceMode, String metricsGroup, MetricRegistry metricRegistry) {
        _serviceMode = serviceMode;
        _metricsGroup = metricsGroup;
        _metricRegistry = metricRegistry;
    }

    @Override
    protected void configure() {

        // Note: we only use ZooKeeper if this is the data center that is allowed to edit table metadata (create/drop table)
        // Chain TableDAO -> MutexTableDAO -> CachingTableDAO -> AstyanaxTableDAO.
        bind(TableDAO.class).to(MutexTableDAO.class).asEagerSingleton();
        bind(TableDAO.class).annotatedWith(MutexTableDAODelegate.class).to(CachingTableDAO.class).asEagerSingleton();
        bind(TableDAO.class).annotatedWith(CachingTableDAODelegate.class).to(AstyanaxTableDAO.class).asEagerSingleton();
        bind(String.class).annotatedWith(SystemTableNamespace.class).toInstance("__system_blob");
        bind(PlacementFactory.class).to(BlobPlacementFactory.class).asEagerSingleton();
        bind(PlacementCache.class).asEagerSingleton();
        bind(AstyanaxTableDAO.class).asEagerSingleton();
        bind(CassandraFactory.class).asEagerSingleton();
        bind(HintsConsistencyTimeProvider.class).asEagerSingleton();
        bind(MinLagConsistencyTimeProvider.class).asEagerSingleton();

        requireBinding(Key.get(String.class, SystemTablePlacement.class));

        // No bootstrap tables are required.  System tables are stored as regular SoR tables.
        bind(new TypeLiteral<Map<String, Long>>() {}).annotatedWith(BootstrapTables.class)
                .toInstance(ImmutableMap.of());

        bind(StorageProvider.class).to(AstyanaxStorageProvider.class).asEagerSingleton();
        bind(MetadataProvider.class).to(AstyanaxStorageProvider.class).asEagerSingleton();
        bind(DataCopyDAO.class).to(AstyanaxStorageProvider.class).asEagerSingleton();
        bind(DataPurgeDAO.class).to(AstyanaxStorageProvider.class).asEagerSingleton();

        bind(String.class).annotatedWith(Maintenance.class).toInstance("blob");

        // The web servers are responsible for performing background table background_table_maintenance.
        if (_serviceMode.specifies(EmoServiceMode.Aspect.background_table_maintenance)) {
            bind(MaintenanceSchedulerManager.class).asEagerSingleton();
            bind(MaintenanceDAO.class).to(AstyanaxTableDAO.class).asEagerSingleton();
            bind(CuratorFramework.class).annotatedWith(Maintenance.class)
                    .to(Key.get(CuratorFramework.class, BlobStoreZooKeeper.class)).asEagerSingleton();
            bind(TableChangesEnabledTask.class).asEagerSingleton();
            bind(MaintenanceRateLimitTask.class).asEagerSingleton();
            bind(MoveTableTask.class).asEagerSingleton();
        }

        // Enable updating the ZooKeeper full consistency data if specified
        if (_serviceMode.specifies(EmoServiceMode.Aspect.blob_zookeeper_full_consistency)) {
            // BlobStore needs to provide HintsPollers and FullConsistencyTimeProvider for its cassandra clusters
            bind(HintsPollerManager.class).asEagerSingleton();
            bind(HintsConsistencyTimeTask.class).asEagerSingleton();
            bind(MinLagDurationTask.class).asEagerSingleton();
            bind(ClusterHintsPoller.class).asEagerSingleton();
        }

        // Explicit bindings so objects don't get created as a just-in-time binding in the root injector.
        // This needs to be done for just about anything that has only public dependencies.
        bind(AstyanaxStorageProvider.class).asEagerSingleton();

        // The DataCenter discovery expects to be able to enumerate BlobStore keyspaces
        if (_serviceMode.specifies(EmoServiceMode.Aspect.dataCenter_announce)) {
            bind(KeyspaceDiscovery.class).annotatedWith(Names.named("blob")).to(AstyanaxKeyspaceDiscovery.class).asEagerSingleton();
            expose(KeyspaceDiscovery.class).annotatedWith(Names.named("blob"));
        }

        // Bind the BlobStore instance that the rest of the application will consume
        bind(DefaultBlobStore.class).asEagerSingleton();
        bind(BlobStore.class).annotatedWith(LocalBlobStore.class).to(DefaultBlobStore.class);
        expose(BlobStore.class);

        bind(AmazonS3Provider.class).asEagerSingleton();
        bind(S3HealthCheck.class).asEagerSingleton();
        // Bind any methods annotated with @ParameterizedTimed
        bindListener(Matchers.any(), new ParameterizedTimedListener(_metricsGroup, _metricRegistry));
    }

    @Provides @Singleton
    BlobStore provideBlobStore(@LocalBlobStore Provider<BlobStore> localBlobStoreProvider,
                               @SystemBlobStore Provider<BlobStore> systemBlobStoreProvider,
                               DataCenterConfiguration dataCenterConfiguration) {
        // Provides the unannotated version of the BlobStore
        // If this is the system data center, return the local BlobStore implementation
        // Otherwise return a proxy that delegates to local or remote system BlobStores
        if (dataCenterConfiguration.isSystemDataCenter()) {
            return localBlobStoreProvider.get();
        } else {
            return new BlobStoreProviderProxy(localBlobStoreProvider, systemBlobStoreProvider);
        }
    }

    @Provides @Singleton
    Optional<TableMutexManager> provideTableMutexManager(DataCenterConfiguration dataCenterConfiguration, @BlobStoreZooKeeper CuratorFramework curator) {
        // We only use ZooKeeper if this is the data center that is allowed to edit table metadata (create/drop table)
        if (dataCenterConfiguration.isSystemDataCenter()) {
            return Optional.of(new TableMutexManager(curator, "/lock/table-partitions"));
        }
        return Optional.empty();
    }

    @Provides @Singleton @CurrentDataCenter
    String provideDataCenters(DataCenterConfiguration dataCenterConfiguration) {
        return dataCenterConfiguration.getCurrentDataCenter();
    }

    @Provides @Singleton @ValidTablePlacements
    Set<String> provideValidTablePlacements(BlobStoreConfiguration configuration) {
        return configuration.getValidTablePlacements();
    }

    @Provides @Singleton @ShardsPerTable
    int provideShardsPerTable(BlobStoreConfiguration configuration) {
        return configuration.getMinimumSplitsPerTable();
    }

    @Provides @Singleton @KeyspaceMap
    Map<String, CassandraKeyspace> provideKeyspaces(BlobStoreConfiguration configuration, CassandraFactory factory) {
        Map<String, CassandraKeyspace> keyspaceMap = Maps.newHashMap();
        for (CassandraConfiguration cassandraConfig : configuration.getCassandraClusters().values()) {
            Map<String, CassandraKeyspace> keyspacesInCluster = factory.build(cassandraConfig);
            keyspaceMap.putAll(keyspacesInCluster);
        }
        return ImmutableMap.copyOf(keyspaceMap);
    }

    @Provides @Singleton @PlacementsUnderMove
    Map<String, String> providePlacementsUnderMove(BlobStoreConfiguration configuration,
                                                   @ValidTablePlacements Set<String> validPlacements) {
        return configuration.getPlacementsUnderMove() == null ? ImmutableMap.of()
                : validateMoveMap(ImmutableMap.copyOf(configuration.getPlacementsUnderMove()), validPlacements);
    }

    /** Required for HintsPollerManager **/

    @Provides @Singleton @CassandraClusters
    Collection<String> provideCassandraClusterNames(BlobStoreConfiguration configuration) {
        Set<String> clusters = Sets.newLinkedHashSet();
        for (CassandraConfiguration config : configuration.getCassandraClusters().values()) {
            clusters.add(config.getCluster());
        }
        return clusters;
    }

    @Provides @Singleton @CQLSessionForHintsPollerMap
    Map<String, HintsPollerCQLSession> provideCQLSessionsForHintsPoller(BlobStoreConfiguration configuration, CassandraFactory factory) {
        Map<String, HintsPollerCQLSession> cqlSessionMap = Maps.newHashMap();
        for (CassandraConfiguration config : configuration.getCassandraClusters().values()) {
            String clusterName = config.getCluster();
            if(!cqlSessionMap.containsKey(clusterName)) {
                cqlSessionMap.put(clusterName, factory.cqlSessionForHintsPoller(config));
            }
        }
        return ImmutableMap.copyOf(cqlSessionMap);
    }

    @Provides @Singleton
    Collection<ClusterInfo> provideCassandraClusterInfo(BlobStoreConfiguration configuration) {
        Map<String, ClusterInfo> clusterInfoMap = Maps.newLinkedHashMap();
        for (CassandraConfiguration config : configuration.getCassandraClusters().values()) {
            ClusterInfo clusterInfo = new ClusterInfo(config.getCluster(), config.getClusterMetric());
            ClusterInfo old = clusterInfoMap.put(config.getCluster(), clusterInfo);
            checkState(old == null || old.getClusterMetric().equals(clusterInfo.getClusterMetric()),
                    "Cluster %s is configured with multiple distinct names for the cluster metric.", config.getCluster());
        }
        return ImmutableList.copyOf(clusterInfoMap.values());
    }

    @Provides @Singleton
    FullConsistencyTimeProvider provideFullConsistencyTimeProvider(Collection<ClusterInfo> cassandraClusterInfo,
                                                                   HintsConsistencyTimeProvider hintsConsistencyTimeProvider,
                                                                   MinLagConsistencyTimeProvider minLagConsistencyTimeProvider,
                                                                   MetricRegistry metricRegistry) {
        return new CompositeConsistencyTimeProvider(cassandraClusterInfo, ImmutableList.of(hintsConsistencyTimeProvider,
                minLagConsistencyTimeProvider), metricRegistry);
    }

    @Provides @Singleton @HintsConsistencyTimeValues
    Map<String, ValueStore<Long>> provideHintsTimestampValues(@CassandraClusters Collection<String> cassandraClusters,
                                                              @GlobalFullConsistencyZooKeeper CuratorFramework curator,
                                                              LifeCycleRegistry lifeCycle) {
        // Create a timestamp holder for each Cassandra cluster.
        Map<String, ValueStore<Long>> valuesByCluster = Maps.newLinkedHashMap();
        for (String cluster : cassandraClusters) {
            String zkPath = ZKPaths.makePath("/consistency/max-timestamp", cluster);
            ZkValueStore<Long> holder = new ZkValueStore<>(curator, zkPath, new ZkTimestampSerializer());
            valuesByCluster.put(cluster, lifeCycle.manage(holder));
        }
        return valuesByCluster;
    }

    @Provides @Singleton @MinLagDurationValues
    Map<String, ValueStore<Duration>> provideMinLagDurationValues(@CassandraClusters Collection<String> cassandraClusters,
                                                                  @GlobalFullConsistencyZooKeeper final CuratorFramework curator,
                                                                  final LifeCycleRegistry lifeCycle) {
        final ConcurrentMap<String, ValueStore<Duration>> valuesByCluster = Maps.newConcurrentMap();
        for (String cluster : cassandraClusters) {
            String zkPath = ZKPaths.makePath("/consistency/min-lag", cluster);
            ZkValueStore<Duration> holder = new ZkValueStore<>(curator, zkPath, new ZkDurationSerializer());
            valuesByCluster.put(cluster, lifeCycle.manage(holder));
        }
        return valuesByCluster;
    }

    @Provides @Singleton @CachingTableDAORegistry
    CacheRegistry provideCacheRegistry(CacheRegistry cacheRegistry) {
        return cacheRegistry.withNamespace("blob");
    }

    @Provides @Singleton @TableChangesEnabled
    ValueStore<Boolean> provideTableChangesEnabled(@BlobStoreZooKeeper CuratorFramework curator,
                                                   LifeCycleRegistry lifeCycle) {
        return lifeCycle.manage(new ZkValueStore<>(curator, "settings/table-changes-enabled", new ZkBooleanSerializer(), true));
    }

    @Provides @Singleton @Maintenance
    MapStore<Double> provideRateLimiterSettings(@BlobStoreZooKeeper CuratorFramework curator,
                                                LifeCycleRegistry lifeCycle) {
        return lifeCycle.manage(new ZkMapStore<>(curator, "maintenance-rate-limits", new ZkDoubleSerializer()));
    }

    @Provides @Singleton @Maintenance
    RateLimiterCache provideRateLimiterCache(@Maintenance MapStore<Double> rateLimits) {
        return new RateLimiterCache(rateLimits, 1000);
    }

    private static Map<String, String> validateMoveMap(Map<String, String> moveMap, Set<String> validPlacements) {
        for (Map.Entry<String, String> entry : moveMap.entrySet()) {
            checkArgument(validPlacements.contains(entry.getKey()), String.format("Invalid move-from placement: %s", entry.getKey()));
            checkArgument(validPlacements.contains(entry.getValue()), String.format("Invalid move-to placement: %s", entry.getValue()));
            // Prevent chained moves
            checkArgument(!moveMap.containsKey(entry.getValue()), "Chained moves are not allowed");
        }
        return moveMap;
    }

    @Provides @Singleton @BlobReadConsistency
    ConsistencyLevel provideBlobReadConsistency(BlobStoreConfiguration configuration) {
        // By default use local quorum
        return Optional.ofNullable(configuration.getReadConsistency()).orElse(ConsistencyLevel.CL_LOCAL_QUORUM);
    }

    @Provides @Singleton @Nullable
    S3Configuration provideS3BucketConfiguration(BlobStoreConfiguration configuration) {
        return configuration.getS3Configuration();
    }

    @Provides @Singleton
    S3HealthCheckConfiguration provideS3HealthCheckConfiguration(@Nullable S3Configuration s3Configuration) {
        if (null != s3Configuration && null != s3Configuration.getS3BucketConfigurations()) {
            return s3Configuration.getS3HealthCheckConfiguration();
        } else {
            return new S3HealthCheckConfiguration();
        }
    }
}
