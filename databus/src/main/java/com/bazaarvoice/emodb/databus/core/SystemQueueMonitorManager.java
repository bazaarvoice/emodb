package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.databus.DataCenterFanoutPartitions;
import com.bazaarvoice.emodb.databus.DatabusZooKeeper;
import com.bazaarvoice.emodb.databus.MasterFanoutPartitions;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.table.db.ClusterInfo;
import com.bazaarvoice.emodb.table.db.consistency.DatabusClusterInfo;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Starts the System queue length monitor, subject to ZooKeeper leader election.
 * Additionally starts the System queue lag monitor.  The lag is the maximum lag of all system queues currently being
 * fanned out by this server, as determined by leadership election.  Therefore it is not subject to leadership election
 * and runs on every server, but the maximum it reports will only be the maximum for those queues with local leadership.
 */
public class SystemQueueMonitorManager {
    @Inject
    SystemQueueMonitorManager(LifeCycleRegistry lifeCycle,
                              final DatabusEventStore eventStore,
                              final DataCenters dataCenters,
                              @DatabusClusterInfo final Collection<ClusterInfo> clusterInfo,
                              @DatabusZooKeeper CuratorFramework curator,
                              @SelfHostAndPort HostAndPort self,
                              @MasterFanoutPartitions int masterFanoutPartitions,
                              @DataCenterFanoutPartitions int dataCenterFanoutPartitions,
                              LeaderServiceTask dropwizardTask,
                              final MetricRegistry metricRegistry) {
        LeaderService leaderService = new LeaderService(
                curator, "/leader/queue-monitor", self.toString(), "Leader-QueueMonitor", 1, TimeUnit.MINUTES,
                new Supplier<Service>() {
                    @Override
                    public Service get() {
                        return new SystemQueueMonitor(eventStore, dataCenters, clusterInfo, masterFanoutPartitions, dataCenterFanoutPartitions, metricRegistry);
                    }
                });
        ServiceFailureListener.listenTo(leaderService, metricRegistry);
        dropwizardTask.register("queue-monitor", leaderService);
        lifeCycle.manage(new ManagedGuavaService(leaderService));

        FanoutLagMonitor fanoutLagMonitor = new FanoutLagMonitor(masterFanoutPartitions, dataCenterFanoutPartitions, dataCenters, metricRegistry);
        lifeCycle.manage(new ManagedGuavaService(fanoutLagMonitor));
    }
}
