package com.bazaarvoice.megabus.tableevents;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.table.db.eventregistry.TableEventTools;
import com.bazaarvoice.emodb.table.db.eventregistry.TableEventRegistry;
import com.bazaarvoice.megabus.guice.MegabusRefTopic;
import com.bazaarvoice.megabus.guice.MegabusZookeeper;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.TimeUnit;

public class TableEventProcessorManager extends LeaderService {


    private static final String SERVICE_NAME = "table-event-processor";
    private static final String LEADER_DIR = "/leader/table-event";

    @Inject
    public TableEventProcessorManager(LeaderServiceTask leaderServiceTask,
                                      @MegabusZookeeper CuratorFramework curator,
                                      @SelfHostAndPort HostAndPort selfHostAndPort,
                                      @TableEventRegistrationId String tableEventRegistrationId,
                                      TableEventRegistry tableEventRegistry,
                                      TableEventTools tableEventTools,
                                      KafkaCluster kafkaCluster,
                                      @MegabusRefTopic Topic refTopic,
                                      ObjectMapper objectMapper,
                                      MetricRegistry metricRegistry) {
        super(curator, LEADER_DIR, selfHostAndPort.toString(), SERVICE_NAME, 10, TimeUnit.MINUTES,
                () -> new TableEventProcessor(tableEventRegistrationId, tableEventRegistry, metricRegistry, tableEventTools,
                        kafkaCluster.producer(), objectMapper, refTopic));
        ServiceFailureListener.listenTo(this, metricRegistry);
        leaderServiceTask.register(SERVICE_NAME, this);
    }
}
