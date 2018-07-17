package com.bazaarvoice.emodb.event;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.metrics.ParameterizedTimedListener;
import com.bazaarvoice.emodb.event.admin.ClaimCountTask;
import com.bazaarvoice.emodb.event.admin.DedupQueueTask;
import com.bazaarvoice.emodb.event.api.ChannelConfiguration;
import com.bazaarvoice.emodb.event.api.DedupEventStore;
import com.bazaarvoice.emodb.event.api.DedupEventStoreChannels;
import com.bazaarvoice.emodb.event.api.EventStore;
import com.bazaarvoice.emodb.event.core.ClaimStore;
import com.bazaarvoice.emodb.event.core.DefaultClaimStore;
import com.bazaarvoice.emodb.event.core.DefaultEventStore;
import com.bazaarvoice.emodb.event.core.MetricsGroupName;
import com.bazaarvoice.emodb.event.db.EventIdSerializer;
import com.bazaarvoice.emodb.event.db.EventReaderDAO;
import com.bazaarvoice.emodb.event.db.EventWriterDAO;
import com.bazaarvoice.emodb.event.db.astyanax.AstyanaxEventIdSerializer;
import com.bazaarvoice.emodb.event.db.astyanax.AstyanaxEventReaderDAO;
import com.bazaarvoice.emodb.event.db.astyanax.AstyanaxEventWriterDAO;
import com.bazaarvoice.emodb.event.db.astyanax.AstyanaxManifestPersister;
import com.bazaarvoice.emodb.event.db.astyanax.DefaultSlabAllocator;
import com.bazaarvoice.emodb.event.db.astyanax.ManifestPersister;
import com.bazaarvoice.emodb.event.db.astyanax.SlabAllocator;
import com.bazaarvoice.emodb.event.db.astyanax.VerifyRandomPartitioner;
import com.bazaarvoice.emodb.event.dedup.DedupQueueAdmin;
import com.bazaarvoice.emodb.event.dedup.DefaultDedupEventStore;
import com.bazaarvoice.emodb.event.owner.OstrichOwnerFactory;
import com.bazaarvoice.emodb.event.owner.OstrichOwnerGroup;
import com.bazaarvoice.emodb.event.owner.OstrichOwnerGroupFactory;
import com.bazaarvoice.emodb.event.owner.OwnerGroup;
import com.bazaarvoice.emodb.sortedq.api.SortedQueue;
import com.bazaarvoice.emodb.sortedq.api.SortedQueueFactory;
import com.bazaarvoice.emodb.sortedq.core.PersistentSortedQueue;
import com.bazaarvoice.emodb.sortedq.db.QueueDAO;
import com.bazaarvoice.emodb.sortedq.db.astyanax.AstyanaxQueueDAO;
import com.bazaarvoice.ostrich.HostDiscovery;
import com.codahale.metrics.MetricRegistry;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.matcher.Matchers;
import org.apache.curator.framework.CuratorFramework;

import javax.annotation.Nullable;
import java.time.Duration;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Guice module for constructing an {@link com.bazaarvoice.emodb.event.api.EventStore}.
 * <p>
 * Requires the following external references:
 * <ul>
 * <li> {@link CassandraKeyspace}
 * <li> {@link ChannelConfiguration}
 * <li> {@link LeaderServiceTask}
 * <li> {@link LifeCycleRegistry}
 * <li> @{@link SelfHostAndPort} {@link HostAndPort}
 * <li> @{@link EventStoreHostDiscovery} {@link HostDiscovery}
 * <li> @{@link EventStoreZooKeeper} {@link CuratorFramework}
 * <li> {@link DedupEventStoreChannels}
 * </ul>
 * Exports the following:
 * <ul>
 * <li> {@link EventStore}
 * <li> {@link DedupEventStore}
 * </ul>
 */
public class EventStoreModule extends PrivateModule {
    private final String _metricsGroup;
    private MetricRegistry _metricRegistry;

    public EventStoreModule(String metricsGroup, MetricRegistry metricRegistry) {
        _metricsGroup = checkNotNull(metricsGroup, "metricsGroup");
        _metricRegistry = metricRegistry;
    }

    @Override
    protected void configure() {
        install(new FactoryModuleBuilder().implement(SortedQueue.class, PersistentSortedQueue.class).build(SortedQueueFactory.class));

        // DAO classes
        bind(AstyanaxEventReaderDAO.class).asEagerSingleton();
        bind(EventReaderDAO.class).to(AstyanaxEventReaderDAO.class).asEagerSingleton();
        bind(EventWriterDAO.class).to(AstyanaxEventWriterDAO.class).asEagerSingleton();
        bind(EventIdSerializer.class).to(AstyanaxEventIdSerializer.class).asEagerSingleton();
        bind(SlabAllocator.class).to(DefaultSlabAllocator.class).asEagerSingleton();
        bind(ManifestPersister.class).to(AstyanaxManifestPersister.class).asEagerSingleton();
        bind(VerifyRandomPartitioner.class).asEagerSingleton();
        bind(QueueDAO.class).to(AstyanaxQueueDAO.class).asEagerSingleton();

        // Core classes
        bind(ClaimStore.class).to(DefaultClaimStore.class).asEagerSingleton();
        bind(EventStore.class).to(DefaultEventStore.class).asEagerSingleton();
        bind(DefaultDedupEventStore.class).asEagerSingleton();
        bind(DedupEventStore.class).to(DefaultDedupEventStore.class).asEagerSingleton();
        bind(DedupQueueAdmin.class).to(DefaultDedupEventStore.class).asEagerSingleton();

        // Public classes
        expose(EventStore.class);
        expose(DedupEventStore.class);
        expose(OstrichOwnerGroupFactory.class);

        // Metrics instrumentation
        bind(String.class).annotatedWith(MetricsGroupName.class).toInstance(_metricsGroup);
        bindListener(Matchers.any(), new ParameterizedTimedListener(_metricsGroup, _metricRegistry));
        bind(ClaimCountTask.class).asEagerSingleton();
        bind(DedupQueueTask.class).asEagerSingleton();
    }

    @Provides @Singleton
    OstrichOwnerGroupFactory provideOwnerServicesFactory(final LifeCycleRegistry lifeCycle,
                                                         @EventStoreZooKeeper final CuratorFramework curator,
                                                         @EventStoreHostDiscovery final HostDiscovery hostDiscovery,
                                                         @SelfHostAndPort final HostAndPort self,
                                                         final LeaderServiceTask dropwizardTask) {
        return new OstrichOwnerGroupFactory() {
            @Override
            public <T extends Service>
            OwnerGroup<T> create(String group, OstrichOwnerFactory<T> factory, @Nullable Duration expireWhenInactive) {
                return lifeCycle.manage(
                        new OstrichOwnerGroup<>(group, factory, expireWhenInactive, curator, hostDiscovery, self, dropwizardTask, _metricRegistry));
            }
        };
    }
}
