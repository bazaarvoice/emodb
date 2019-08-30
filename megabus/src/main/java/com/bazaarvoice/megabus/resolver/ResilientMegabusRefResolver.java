package com.bazaarvoice.megabus.resolver;

import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.megabus.MegabusApplicationId;
import com.bazaarvoice.megabus.MegabusRefTopic;
import com.bazaarvoice.megabus.MegabusTopic;
import com.bazaarvoice.megabus.MissingRefTopic;
import com.bazaarvoice.megabus.RetryRefTopic;
import com.bazaarvoice.megabus.service.ResilientService;
import com.codahale.metrics.MetricRegistry;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import java.time.Clock;
import java.time.Duration;

public class ResilientMegabusRefResolver extends ResilientService {
    private static final String SERVICE_NAME = "resilient-megabus-ref-resolver";
    private static Duration RESTART_DELAY = Duration.ofSeconds(30);


    @Inject
    public ResilientMegabusRefResolver(DataProvider dataProvider, @MegabusRefTopic Topic megabusRefTopic,
                              @MegabusTopic Topic megabusResolvedTopic,
                              @RetryRefTopic Topic retryRefTopic,
                              @MissingRefTopic Topic missingRefTopic,
                              @MegabusApplicationId String applicationId,
                              KafkaCluster kafkaCluster, Clock clock,
                              @SelfHostAndPort HostAndPort hostAndPort,
                              MetricRegistry metricRegistry) {
        super(SERVICE_NAME,
                () -> new MegabusRefResolver(dataProvider, megabusRefTopic, megabusResolvedTopic, retryRefTopic, missingRefTopic, applicationId, kafkaCluster, clock, hostAndPort, metricRegistry),
                RESTART_DELAY, false);
    }
}
