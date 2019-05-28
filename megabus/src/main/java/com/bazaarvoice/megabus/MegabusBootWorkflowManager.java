package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;

public class MegabusBootWorkflowManager {


    private static final String SERVICE_NAME = "megabus-boot-manager";
    private static final String LEADER_DIR = "/leader/megabus";

    @Inject
    public MegabusBootWorkflowManager(CuratorFramework curator, @SelfHostAndPort HostAndPort selfHostAndPort,
                                      LifeCycleRegistry lifecycle, LeaderServiceTask leaderServiceTask,
                                      MetricRegistry metricRegistry[]) {

//        super(curator, LEADER_DIR, selfHostAndPort.toString(), SERVICE_NAME, 1, TimeUnit.MINUTES,
//                new Supplier<Service>() {
//                    @Override
//                    public Service get() {
//                        return new LocalMigratorMonitor(workflow, statusDAO, dataTools);
//                    }
//                });
//
//        ServiceFailureListener.listenTo(this, metricRegistry);
//        leaderServiceTask.register(SERVICE_NAME, this);
//        lifecycle.manage(new ManagedGuavaService(this));
    }
}
