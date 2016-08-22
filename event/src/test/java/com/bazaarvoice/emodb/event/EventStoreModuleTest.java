package com.bazaarvoice.emodb.event;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.event.api.ChannelConfiguration;
import com.bazaarvoice.emodb.event.api.DedupEventStoreChannels;
import com.bazaarvoice.emodb.event.api.EventStore;
import com.bazaarvoice.emodb.event.core.DefaultClaimStore;
import com.bazaarvoice.emodb.event.db.astyanax.AstyanaxEventReaderDAO;
import com.bazaarvoice.emodb.event.db.astyanax.AstyanaxManifestPersister;
import com.bazaarvoice.emodb.event.db.astyanax.DefaultSlabAllocator;
import com.bazaarvoice.ostrich.HostDiscovery;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import org.apache.curator.framework.CuratorFramework;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class EventStoreModuleTest {

    @Test
    public void testEventStoreModule() {
        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                binder().requireExplicitBindings();

                bind(ChannelConfiguration.class).toInstance(mock(ChannelConfiguration.class));
                bind(CassandraKeyspace.class).toInstance(mock(CassandraKeyspace.class));
                bind(LeaderServiceTask.class).toInstance(mock(LeaderServiceTask.class));
                bind(LifeCycleRegistry.class).toInstance(new SimpleLifeCycleRegistry());
                bind(TaskRegistry.class).toInstance(mock(TaskRegistry.class));
                bind(HostAndPort.class).annotatedWith(SelfHostAndPort.class).toInstance(HostAndPort.fromString("localhost:8080"));
                bind(CuratorFramework.class).annotatedWith(EventStoreZooKeeper.class).toInstance(mock(CuratorFramework.class));
                bind(HostDiscovery.class).annotatedWith(EventStoreHostDiscovery.class).toInstance(mock(HostDiscovery.class));
                bind(DedupEventStoreChannels.class).toInstance(DedupEventStoreChannels.isolated(":__dedupq_write", ":__dedupq_read"));
                bind(new TypeLiteral<Supplier<Boolean>>() {}).annotatedWith(DedupEnabled.class).toInstance(Suppliers.ofInstance(true));

                MetricRegistry metricRegistry = new MetricRegistry();
                bind(MetricRegistry.class).toInstance(metricRegistry);

                install(new EventStoreModule("bv.event", metricRegistry));
            }
        });

        EventStore eventStore = injector.getInstance(EventStore.class);

        assertNotNull(eventStore);

        // Verify that some things we expect to be private are, indeed, private
        assertPrivate(injector, AstyanaxEventReaderDAO.class);
        assertPrivate(injector, AstyanaxManifestPersister.class);
        assertPrivate(injector, DefaultSlabAllocator.class);
        assertPrivate(injector, DefaultClaimStore.class);
    }

    private void assertPrivate(Injector injector, Class<?> type) {
        try {
            injector.getInstance(type);
            fail();
        } catch (ConfigurationException e) {
            // Expected
        }
    }
}
