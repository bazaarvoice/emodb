package com.bazaarvoice.emodb.queue;

import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.common.cassandra.KeyspaceConfiguration;
import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.queue.api.DedupQueueService;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.ostrich.HostDiscovery;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.curator.framework.CuratorFramework;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;

public class QueueModuleTest {

    @Test
    public void testQueueModule() {
        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                binder().requireExplicitBindings();

                // construct the minimum necessary elements to allow a Queue module to be created.
                bind(QueueConfiguration.class).toInstance(new QueueConfiguration()
                        .setCassandraConfiguration(new CassandraConfiguration()
                                .setCluster("Test Cluster")
                                .setSeeds("127.0.0.1")
                                .setPartitioner("random")
                                .setKeyspaces(ImmutableMap.of(
                                        "queue", new KeyspaceConfiguration().setHealthCheckColumnFamily("manifest")))));

                bind(HealthCheckRegistry.class).toInstance(mock(HealthCheckRegistry.class));
                bind(LeaderServiceTask.class).toInstance(mock(LeaderServiceTask.class));
                bind(LifeCycleRegistry.class).toInstance(new SimpleLifeCycleRegistry());
                bind(TaskRegistry.class).toInstance(mock(TaskRegistry.class));
                bind(HostAndPort.class).annotatedWith(SelfHostAndPort.class).toInstance(HostAndPort.fromString("localhost:8080"));
                bind(CuratorFramework.class).annotatedWith(Global.class).toInstance(mock(CuratorFramework.class));
                bind(CuratorFramework.class).annotatedWith(QueueZooKeeper.class).toInstance(mock(CuratorFramework.class));
                bind(HostDiscovery.class).annotatedWith(DedupQueueHostDiscovery.class).toInstance(mock(HostDiscovery.class));
                bind(JobHandlerRegistry.class).toInstance(mock(JobHandlerRegistry.class));
                bind(JobService.class).toInstance(mock(JobService.class));

                MetricRegistry metricRegistry = new MetricRegistry();
                bind(MetricRegistry.class).toInstance(metricRegistry);

                install(new QueueModule(metricRegistry));
            }
        });

        assertNotNull(injector.getInstance(QueueService.class));
        assertNotNull(injector.getInstance(DedupQueueService.class));
    }
}
