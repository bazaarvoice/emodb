package com.bazaarvoice.megabus.resolver;

import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.megabus.guice.DelayProcessorConsumerGroup;
import com.bazaarvoice.megabus.guice.MissingRefTopic;
import com.bazaarvoice.megabus.guice.RetryRefTopic;
import com.bazaarvoice.megabus.service.ResilientService;
import com.codahale.metrics.MetricRegistry;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import java.time.Clock;
import java.time.Duration;

public class ResilientMissingRefDelayProcessor extends ResilientService {

    private static String SERVICE_NAME = "resilient-missing-ref-delay-processor";
    private static Duration RESTART_DELAY = Duration.ofSeconds(30);

    @Inject
    public ResilientMissingRefDelayProcessor(DataProvider dataProvider,
                                             @RetryRefTopic Topic retryRefTopic,
                                             @MissingRefTopic Topic missingRefTopic,
                                             KafkaCluster kafkaCluster, Clock clock,
                                             @SelfHostAndPort HostAndPort hostAndPort,
                                             @DelayProcessorConsumerGroup String delayProcessorConsumerGroup,
                                             MetricRegistry metricRegistry) {
        super(SERVICE_NAME,
                () -> new MissingRefDelayProcessor(dataProvider, retryRefTopic, missingRefTopic,
                        kafkaCluster, clock, hostAndPort, delayProcessorConsumerGroup, metricRegistry),
                RESTART_DELAY, false);
    }
}
