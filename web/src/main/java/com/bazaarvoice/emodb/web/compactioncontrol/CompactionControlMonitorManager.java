package com.bazaarvoice.emodb.web.compactioncontrol;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.compactioncontrol.LocalCompactionControl;
import com.bazaarvoice.emodb.table.db.consistency.GlobalFullConsistencyZooKeeper;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.TimeUnit;

/**
 * Starts the stash run time monitor, subject to ZooKeeper leader election.
 */
public class CompactionControlMonitorManager {
    @Inject
    CompactionControlMonitorManager(LifeCycleRegistry lifeCycle,
                                    @LocalCompactionControl CompactionControlSource compactionControlSource,
                                    DataCenters dataCenters,
                                    @GlobalFullConsistencyZooKeeper CuratorFramework curator,
                                    @SelfHostAndPort HostAndPort self,
                                    LeaderServiceTask dropwizardTask,
                                    final MetricRegistry metricRegistry) {
        LeaderService leaderService = new LeaderService(
                curator, "/leader/compaction-control-monitor", self.toString(), "Leader-CompactionControlMonitor", 30, TimeUnit.MINUTES,
                new Supplier<Service>() {
                    @Override
                    public Service get() {
                        return new CompactionControlMonitor(compactionControlSource, dataCenters);
                    }
                });
        ServiceFailureListener.listenTo(leaderService, metricRegistry);
        dropwizardTask.register("stash-runtime-monitor", leaderService);
        lifeCycle.manage(new ManagedGuavaService(leaderService));
    }
}