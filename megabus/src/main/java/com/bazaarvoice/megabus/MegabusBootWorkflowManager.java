package com.bazaarvoice.megabus;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;

public class MegabusBootWorkflowManager extends LeaderService {


    private static final String SERVICE_NAME = "megabus-boot-manager";
    private static final String LEADER_DIR = "/leader/megabus";

    @Inject
    public MegabusBootWorkflowManager(@MegabusZookeeper CuratorFramework curator, @SelfHostAndPort HostAndPort selfHostAndPort,
                                      LifeCycleRegistry lifecycle, LeaderServiceTask leaderServiceTask,
                                      MetricRegistry metricRegistry, MegabusBootDAO megabusBootDAO, KafkaCluster kafkaCluster,
                                      @MegabusApplicationId String megabusApplicationId, @MegabusTopic Topic megabusTopic) {
        super(curator, LEADER_DIR, selfHostAndPort.toString(), SERVICE_NAME, 1, TimeUnit.MINUTES,
                () -> new MegabusBookWorkflow(kafkaCluster, megabusBootDAO, megabusApplicationId, megabusTopic));
//
        ServiceFailureListener.listenTo(this, metricRegistry);
        leaderServiceTask.register(SERVICE_NAME, this);
        lifecycle.manage(new ManagedGuavaService(this));
    }
}
