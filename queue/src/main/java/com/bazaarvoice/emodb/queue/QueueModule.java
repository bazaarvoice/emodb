package com.bazaarvoice.emodb.queue;

import com.bazaarvoice.emodb.common.cassandra.CassandraFactory;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.event.DedupEnabled;
import com.bazaarvoice.emodb.event.EventStoreHostDiscovery;
import com.bazaarvoice.emodb.event.EventStoreModule;
import com.bazaarvoice.emodb.event.EventStoreZooKeeper;
import com.bazaarvoice.emodb.event.api.ChannelConfiguration;
import com.bazaarvoice.emodb.event.api.DedupEventStoreChannels;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.queue.api.DedupQueueService;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.emodb.queue.core.DefaultDedupQueueService;
import com.bazaarvoice.emodb.queue.core.DefaultQueueService;
import com.bazaarvoice.emodb.queue.core.QueueChannelConfiguration;
import com.bazaarvoice.emodb.queue.core.kafka.KafkaAdminService;
import com.bazaarvoice.emodb.queue.core.kafka.KafkaProducerService;
import com.bazaarvoice.emodb.queue.core.stepfn.StepFunctionService;
import com.bazaarvoice.ostrich.HostDiscovery;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.net.HostAndPort;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.timgroup.statsd.StatsDClient;
import org.apache.curator.framework.CuratorFramework;

import java.time.Clock;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Guice module for constructing a {@link QueueService}.
 * <p>
 * Requires the following external references:
 * <ul>
 * <li> {@link QueueConfiguration}
 * <li> {@link HealthCheckRegistry}
 * <li> {@link LeaderServiceTask}
 * <li> {@link LifeCycleRegistry}
 * <li> {@link TaskRegistry}
 * <li> {@link JobHandlerRegistry}
 * <li> {@link JobService}
 * <li> @{@link SelfHostAndPort} {@link HostAndPort}
 * <li> @{@link DedupQueueHostDiscovery} {@link HostDiscovery}
 * <li> @{@link QueueZooKeeper} {@link CuratorFramework}
 * <li> @{@link Global} {@link CuratorFramework}
 * <li> {@link Clock}
 * </ul>
 * Exports the following:
 * <ul>
 * <li> {@link QueueService}
 * <li> {@link DedupQueueService}
 * </ul>
 */
public class QueueModule extends PrivateModule {

    private MetricRegistry _metricRegistry;

    public QueueModule(MetricRegistry metricRegistry) {
        _metricRegistry = metricRegistry;
    }

    @Override
    protected void configure() {
        bind(CassandraFactory.class).asEagerSingleton();
        bind(StatsDClient.class).asEagerSingleton();

        // Event Store
        bind(ChannelConfiguration.class).to(QueueChannelConfiguration.class).asEagerSingleton();
        bind(CuratorFramework.class).annotatedWith(EventStoreZooKeeper.class).to(Key.get(CuratorFramework.class, QueueZooKeeper.class));
        bind(HostDiscovery.class).annotatedWith(EventStoreHostDiscovery.class).to(Key.get(HostDiscovery.class, DedupQueueHostDiscovery.class));
        bind(DedupEventStoreChannels.class).toInstance(DedupEventStoreChannels.isolated("__dedupq_write:", "__dedupq_read:"));
        bind(new TypeLiteral<Supplier<Boolean>>() {}).annotatedWith(DedupEnabled.class).toInstance(Suppliers.ofInstance(true));
        install(new EventStoreModule("bv.emodb.queue", _metricRegistry));

        // Bind Kafka services
        bind (KafkaAdminService.class).asEagerSingleton();
        bind(KafkaProducerService.class).asEagerSingleton();

        // Bind Step Function Service
        bind(StepFunctionService.class).asEagerSingleton();


        // Bind the Queue instance that the rest of the application will consume
        bind(QueueService.class).to(DefaultQueueService.class).asEagerSingleton();
        expose(QueueService.class);

        // Bind the DedupQueue instance that the rest of the application will consume
        bind(DedupQueueService.class).to(DefaultDedupQueueService.class).asEagerSingleton();
        expose(DedupQueueService.class);

    }

    @Provides @Singleton
    CassandraKeyspace provideKeyspace(QueueConfiguration configuration, CassandraFactory factory) {
        Map<String, CassandraKeyspace> keyspaces = factory.build(configuration.getCassandraConfiguration());
        // Queue module should only have one keyspace configured
        checkArgument(keyspaces.size() == 1, "Only one keyspace expected for queue, found %s", keyspaces.keySet());
        return keyspaces.values().iterator().next();
    }
}
