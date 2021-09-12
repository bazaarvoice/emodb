package com.bazaarvoice.megabus.refproducer;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.bazaarvoice.megabus.MegabusApplicationId;
import com.bazaarvoice.megabus.guice.MegabusZookeeper;
import com.codahale.metrics.MetricRegistry;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;

public class MegabusRefSubscriptionMonitorManager {

    private static final String LEADER_DIR = "/leader/ref-subscription-monitor";

    @Inject
    MegabusRefSubscriptionMonitorManager(LifeCycleRegistry lifeCycle,
                                         final DatabusEventStore eventStore,
                                         @MegabusZookeeper CuratorFramework curator,
                                         @SelfHostAndPort HostAndPort self,
                                         LeaderServiceTask dropwizardTask,
                                         final MetricRegistry metricRegistry,
                                         @MegabusApplicationId String applicationId,
                                         @NumRefPartitions int numRefPartitions) {
        LeaderService leaderService = new LeaderService(
                curator, LEADER_DIR, self.toString(), "Leader-RefSubscriptionMonitor", 1, TimeUnit.MINUTES,
                () -> new MegabusRefSubscriptionMonitor(eventStore, metricRegistry, applicationId, numRefPartitions));
        ServiceFailureListener.listenTo(leaderService, metricRegistry);
        dropwizardTask.register("ref-subscription-monitor", leaderService);
        lifeCycle.manage(new ManagedGuavaService(leaderService));
    }
}
