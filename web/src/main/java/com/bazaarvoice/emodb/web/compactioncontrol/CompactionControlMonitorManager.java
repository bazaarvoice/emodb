package com.bazaarvoice.emodb.web.compactioncontrol;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.compactioncontrol.LocalCompactionControl;
import com.bazaarvoice.emodb.table.db.consistency.GlobalFullConsistencyZooKeeper;
import com.codahale.metrics.MetricRegistry;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

/**
 * Starts the stash run time monitor, subject to ZooKeeper leader election.
 */
public class CompactionControlMonitorManager {
    @Inject
    CompactionControlMonitorManager(LifeCycleRegistry lifeCycle,
                                    @LocalCompactionControl CompactionControlSource compactionControlSource,
                                    @GlobalFullConsistencyZooKeeper CuratorFramework curator,
                                    @SelfHostAndPort HostAndPort self,
                                    Clock clock,
                                    LeaderServiceTask dropwizardTask,
                                    final MetricRegistry metricRegistry) {
        LeaderService leaderService = new LeaderService(
                curator,
                "/leader/compaction-control-monitor",
                self.toString(),
                "Leader-CompactionControlMonitor",
                30, TimeUnit.MINUTES,
                () -> new CompactionControlMonitor(compactionControlSource, clock, metricRegistry)
        );

        ServiceFailureListener.listenTo(leaderService, metricRegistry);
        dropwizardTask.register("stash-runtime-monitor", leaderService);
        lifeCycle.manage(new ManagedGuavaService(leaderService));
    }
}