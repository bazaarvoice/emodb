package com.bazaarvoice.emodb.sor;

import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.common.cassandra.CassandraFactory;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.cassandra.CqlDriverConfiguration;
import com.bazaarvoice.emodb.common.cassandra.cqldriver.HintsPollerCQLSession;
import com.bazaarvoice.emodb.common.dropwizard.guice.SystemTablePlacement;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
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
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.datacenter.api.KeyspaceDiscovery;
import com.bazaarvoice.emodb.sor.admin.RowKeyTask;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.*;
import com.bazaarvoice.emodb.sor.core.DefaultHistoryStore;
import com.bazaarvoice.emodb.sor.db.astyanax.DAOModule;
import com.bazaarvoice.emodb.sor.db.astyanax.DeltaPlacementFactory;
import com.bazaarvoice.emodb.sor.db.cql.CqlForMultiGets;
import com.bazaarvoice.emodb.sor.db.cql.CqlForScans;
import com.bazaarvoice.emodb.sor.db.cql.SorCqlSettingsTask;
import com.bazaarvoice.emodb.sor.log.LogbackSlowQueryLogProvider;
import com.bazaarvoice.emodb.sor.log.SlowQueryLog;
import com.bazaarvoice.emodb.sor.log.SlowQueryLogConfiguration;
import com.bazaarvoice.emodb.table.db.ClusterInfo;
import com.bazaarvoice.emodb.table.db.Placements;
import com.bazaarvoice.emodb.table.db.ShardsPerTable;
import com.bazaarvoice.emodb.table.db.StashBlackListTableCondition;
import com.bazaarvoice.emodb.table.db.StashTableDAO;
import com.bazaarvoice.emodb.table.db.TableBackingStore;
import com.bazaarvoice.emodb.table.db.TableChangesEnabled;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.bazaarvoice.emodb.table.db.astyanax.*;
import com.bazaarvoice.emodb.table.db.consistency.CassandraClusters;
import com.bazaarvoice.emodb.table.db.consistency.ClusterHintsPoller;
import com.bazaarvoice.emodb.table.db.consistency.CompositeConsistencyTimeProvider;
import com.bazaarvoice.emodb.table.db.consistency.DatabusClusterInfo;
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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.inject.Exposed;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.sun.jersey.api.client.Client;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Guice module for constructing a {@link DataStore}.
 * <p>
 * Requires the following external references:
 * <ul>
 * <li> {@link DataStoreConfiguration}
 * <li> {@link DataCenterConfiguration}
 * <li> {@link DataCenters}
 * <li> {@link CacheRegistry}
 * <li> {@link HealthCheckRegistry}
 * <li> {@link LeaderServiceTask}
 * <li> {@link LifeCycleRegistry}
 * <li> {@link TaskRegistry}
 * <li> Curator {@link CuratorFramework}
 * <li> Jersey {@link Client}
 * <li> @{@link CqlForMultiGets} Supplier&lt;Boolean&gt;
 * <li> @{@link CqlForScans} Supplier&lt;Boolean&gt;
 * <li> {@link CqlDriverConfiguration}
 * <li> {@link Clock}
 * <li> {@link CompactionControlSource}
 * </ul>
 * Exports the following:
 * <ul>
 * <li> {@link DataStore}
 * <li> {@link DataProvider}
 * <li> {@link DataTools}
 * </ul>
 */
public class DataStoreModule extends PrivateModule {
    private final EmoServiceMode _serviceMode;

    public DataStoreModule(EmoServiceMode serviceMode) {
        _serviceMode = serviceMode;
    }

    @Override
    protected void configure() {

        requireBinding(Key.get(String.class, SystemTablePlacement.class));

        // Note: we only use ZooKeeper if this is the data center that is allowed to edit table metadata (create/drop table)
        // Chain TableDAO -> MutexTableDAO -> CachingTableDAO -> AstyanaxTableDAO.
        bind(TableDAO.class).to(MutexTableDAO.class).asEagerSingleton();
        bind(TableDAO.class).annotatedWith(MutexTableDAODelegate.class).to(CachingTableDAO.class).asEagerSingleton();
        bind(TableDAO.class).annotatedWith(CachingTableDAODelegate.class).to(AstyanaxTableDAO.class).asEagerSingleton();
        bind(String.class).annotatedWith(SystemTableNamespace.class).toInstance("__system_sor");
        bind(PlacementFactory.class).to(DeltaPlacementFactory.class).asEagerSingleton();
        bind(DeltaPlacementFactory.class).asEagerSingleton();
        bind(PlacementCache.class).asEagerSingleton();
        bind(AstyanaxTableDAO.class).asEagerSingleton();
        bind(CassandraFactory.class).asEagerSingleton();
        bind(SlowQueryLog.class).toProvider(LogbackSlowQueryLogProvider.class);
        bind(HintsConsistencyTimeProvider.class).asEagerSingleton();
        bind(MinLagConsistencyTimeProvider.class).asEagerSingleton();

        // The web servers are responsible for updating the ZooKeeper full consistency data.  CLI tools don't need to.
        // Enable updating the ZooKeeper full consistency data if specified
        if (_serviceMode.specifies(EmoServiceMode.Aspect.sor_zookeeper_full_consistency)) {
            bind(HintsPollerManager.class).asEagerSingleton();
            bind(HintsConsistencyTimeTask.class).asEagerSingleton();
            bind(MinLagDurationTask.class).asEagerSingleton();
            bind(ClusterHintsPoller.class).asEagerSingleton();
        }

        // The web servers are responsible for performing background table background_table_maintenance.
        // Enable background table background_table_maintenance if specified.
        if (_serviceMode.specifies(EmoServiceMode.Aspect.background_table_maintenance)) {
            bind(MaintenanceSchedulerManager.class).asEagerSingleton();
            bind(MaintenanceDAO.class).to(AstyanaxTableDAO.class).asEagerSingleton();
            bind(CuratorFramework.class).annotatedWith(Maintenance.class)
                    .to(Key.get(CuratorFramework.class, DataStoreZooKeeper.class)).asEagerSingleton();
            bind(String.class).annotatedWith(Maintenance.class).toInstance("sor");
            bind(TableChangesEnabledTask.class).asEagerSingleton();
            bind(MaintenanceRateLimitTask.class).asEagerSingleton();
            bind(MoveTableTask.class).asEagerSingleton();
        }

        // Stash requires an additional DAO for storing Stash artifacts and provides a custom interface for access.
        if (_serviceMode.specifies(EmoServiceMode.Aspect.scanner)) {
            bind(CQLStashTableDAO.class).asEagerSingleton();
            bind(StashTableDAO.class).to(AstyanaxTableDAO.class).asEagerSingleton();
            expose(StashTableDAO.class);
        }

        // The system of record requires two bootstrap tables in which it stores its metadata about tables.
        // The 64-bit UUID values were chosen at random.
        bind(new TypeLiteral<Map<String, Long>>() {}).annotatedWith(BootstrapTables.class).toInstance(ImmutableMap.of(
                "__system_sor:table", 0x09d7f33f08984b67L,
                "__system_sor:table_uuid", 0xab33556547b99d25L,
                "__system_sor:data_center", 0x33f1f082cffc2c2fL,
                "__system_sor:table_unpublished_databus_events", 0x44ab556547b99dffL));

        // Bind all DAOs from the DAO module
        install(new DAOModule());

        bind(HistoryStore.class).to(DefaultHistoryStore.class).asEagerSingleton();
        
        // The LocalDataStore annotation binds to the default implementation
        // The unannotated version of DataStore provided below is what the rest of the application will consume
        bind(DefaultDataStore.class).asEagerSingleton();
        bind(DataStore.class).annotatedWith(LocalDataStore.class).to(DefaultDataStore.class);
        expose(DataStore.class);

        // The AstyanaxTableDAO class uses the DataStore (recursively) to store table metadata
        bind(TableBackingStore.class).to(DefaultDataStore.class);
        expose(TableBackingStore.class);

        // Publish events to listeners like the Databus via an instance of EventBus
        bind(EventBus.class).asEagerSingleton();
        expose(EventBus.class);
        // The Databus uses a back-door API to the DataStore.
        bind(DataProvider.class).to(DefaultDataStore.class);
        expose(DataProvider.class);

        // The Databus monitors each placement via a "databus canary" subscription.  It needs placement metadata.
        bind(Placements.class).to(DeltaPlacementFactory.class).asEagerSingleton();
        expose(Placements.class);

        // The DataCenter discovery expects to be able to enumerate SoR keyspaces
        if (_serviceMode.specifies(EmoServiceMode.Aspect.dataCenter_announce)) {
            bind(KeyspaceDiscovery.class).annotatedWith(Names.named("sor")).to(AstyanaxKeyspaceDiscovery.class).asEagerSingleton();
            expose(KeyspaceDiscovery.class).annotatedWith(Names.named("sor"));
        }

        // Administration tasks
        bind(RowKeyTask.class).asEagerSingleton();
        bind(SorCqlSettingsTask.class).asEagerSingleton();

        // Data tools used to generate reports
        bind(DataTools.class).to(DefaultDataStore.class);
        expose(DataTools.class);

        bind(MigratorTools.class).to(DefaultMigratorTools.class);
        expose(MigratorTools.class);
    }

    @Provides @Singleton
    Optional<TableMutexManager> provideTableMutexManager(DataCenterConfiguration dataCenterConfiguration, @DataStoreZooKeeper CuratorFramework curator) {
        // We only use ZooKeeper if this is the data center that is allowed to edit table metadata (create/drop table)
        if (dataCenterConfiguration.isSystemDataCenter()) {
            return Optional.of(new TableMutexManager(curator, "/lock/table-partitions"));
        }
        return Optional.absent();
    }

    @Provides @Singleton
    DataStore provideDataStore(@LocalDataStore Provider<DataStore> localDataStoreProvider,
                               @SystemDataStore Provider<DataStore> systemDataStoreProvider,
                               DataCenterConfiguration dataCenterConfiguration) {
        // Provides the unannotated version of the DataStore
        // If this is the system data center, return the local DataStore implementation
        // Otherwise return a proxy that delegates to local or remote system DataStores
        if (dataCenterConfiguration.isSystemDataCenter()) {
            return localDataStoreProvider.get();
        } else {
            return new DataStoreProviderProxy(localDataStoreProvider, systemDataStoreProvider);
        }
    }

    @Provides @Singleton @DeltaHistoryTtl
    Duration provideDeltaHistoryTtl(DataStoreConfiguration configuration) {
        return configuration.getHistoryTtl();
    }

    @Provides @Singleton @CurrentDataCenter
    String provideDataCenters(DataCenterConfiguration dataCenterConfiguration) {
        return dataCenterConfiguration.getCurrentDataCenter();
    }

    @Provides @Singleton @ValidTablePlacements
    Set<String> provideValidTablePlacements(DataStoreConfiguration configuration) {
        return configuration.getValidTablePlacements();
    }

    @Provides @Singleton @ShardsPerTable
    int provideShardsPerTable(DataStoreConfiguration configuration) {
        return configuration.getMinimumSplitsPerTable();
    }

    @Provides @Singleton @KeyspaceMap
    Map<String, CassandraKeyspace> provideKeyspaces(DataStoreConfiguration configuration, CassandraFactory factory) {
        Map<String, CassandraKeyspace> keyspaceMap = Maps.newHashMap();
        for (CassandraConfiguration cassandraConfig : configuration.getCassandraClusters().values()) {
            Map<String, CassandraKeyspace> keyspacesInCluster = factory.build(cassandraConfig);
            keyspaceMap.putAll(keyspacesInCluster);
        }
        return ImmutableMap.copyOf(keyspaceMap);
    }

    @Provides @Singleton @PlacementsUnderMove
    Map<String, String> providePlacementsUnderMove(DataStoreConfiguration configuration,
                                                   @ValidTablePlacements Set<String> validPlacements) {
        return configuration.getPlacementsUnderMove() == null ? ImmutableMap.<String, String>of()
                : validateMoveMap(ImmutableMap.copyOf(configuration.getPlacementsUnderMove()), validPlacements);
    }

    @Provides @Singleton @CachingTableDAORegistry
    CacheRegistry provideCacheRegistry(CacheRegistry cacheRegistry) {
        return cacheRegistry.withNamespace("sor");
    }

    @Provides @Singleton
    SlowQueryLogConfiguration provideSlowQueryLogConfiguration(DataStoreConfiguration configuration) {
        return configuration.getSlowQueryLogConfiguration();
    }

    @Provides @Singleton @CassandraClusters
    Collection<String> provideCassandraClusterNames(DataStoreConfiguration configuration) {
        Set<String> clusters = Sets.newLinkedHashSet();
        for (CassandraConfiguration config : configuration.getCassandraClusters().values()) {
            clusters.add(config.getCluster());
        }
        return clusters;
    }

    @Provides @Singleton @CQLSessionForHintsPollerMap
    Map<String, HintsPollerCQLSession> provideCQLSessionsForHintsPoller(DataStoreConfiguration configuration, CassandraFactory factory) {
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
    Collection<ClusterInfo> provideCassandraClusterInfoForConsistency(DataStoreConfiguration configuration) {
        return getClusterInfos(configuration);
    }

    // Expose this provider so that DatabusModule can wire in this annotation, which only cares about SoR clusters
    @Provides @Singleton @Exposed @DatabusClusterInfo
    Collection<ClusterInfo> provideCassandraClusterInfo(Collection<ClusterInfo> clusterInfos) {
        return clusterInfos;
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
                                                              LifeCycleRegistry lifeCycle)
            throws Exception {
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

    @Provides @Singleton @TableChangesEnabled
    ValueStore<Boolean> provideTableChangesEnabled(@DataStoreZooKeeper CuratorFramework curator,
                                                   LifeCycleRegistry lifeCycle) {
        return lifeCycle.manage(new ZkValueStore<>(curator, "settings/table-changes-enabled", new ZkBooleanSerializer(), true));
    }

    @Provides @Singleton @Maintenance
    MapStore<Double> provideRateLimiterSettings(@DataStoreZooKeeper CuratorFramework curator,
                                                LifeCycleRegistry lifeCycle) {
        return lifeCycle.manage(new ZkMapStore<>(curator, "background_table_maintenance-rate-limits", new ZkDoubleSerializer()));
    }

    @Provides @Singleton @Maintenance
    RateLimiterCache provideRateLimiterCache(@Maintenance MapStore<Double> rateLimits) {
        return new RateLimiterCache(rateLimits, 1000);
    }

    @Provides @Singleton @StashRoot
    Optional<URI> provideStashRootDirectory(DataStoreConfiguration configuration) {
        if (configuration.getStashRoot().isPresent()) {
            return Optional.of(URI.create(configuration.getStashRoot().get()));
        }
        return Optional.absent();
    }

    @Provides @Singleton @StashBlackListTableCondition
    protected Condition provideStashBlackListTableCondition(DataStoreConfiguration configuration) {
        return configuration.getStashBlackListTableCondition()
                .transform(Conditions::fromString)
                .or(Conditions.alwaysFalse());
    }

    private Collection<ClusterInfo> getClusterInfos(DataStoreConfiguration configuration) {
        Map<String, ClusterInfo> clusterInfoMap = Maps.newLinkedHashMap();
        for (CassandraConfiguration config : configuration.getCassandraClusters().values()) {
            ClusterInfo clusterInfo = new ClusterInfo(config.getCluster(), config.getClusterMetric());
            ClusterInfo old = clusterInfoMap.put(config.getCluster(), clusterInfo);
            checkState(old == null || old.getClusterMetric().equals(clusterInfo.getClusterMetric()),
                    "Cluster %s is configured with multiple distinct names for the cluster metric.", config.getCluster());
        }
        return ImmutableList.copyOf(clusterInfoMap.values());
    }

    private Map<String, String> validateMoveMap(Map<String, String> moveMap, Set<String> validPlacements) {
        for (Map.Entry<String, String> entry : moveMap.entrySet()) {
            checkArgument(validPlacements.contains(entry.getKey()), "Invalid move-from placement");
            checkArgument(validPlacements.contains(entry.getValue()), "Invalid move-to placement");
            // Prevent chained moves
            checkArgument(!moveMap.containsKey(entry.getValue()), "Chained moves are not allowed");
        }
        return moveMap;
    }
}
