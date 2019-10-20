package com.bazaarvoice.megabus.resolver;

import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.megabus.MegabusConfiguration;
import com.bazaarvoice.megabus.guice.MegabusRefTopic;
import com.bazaarvoice.megabus.guice.MegabusTopic;
import com.bazaarvoice.megabus.guice.MissingRefTopic;
import com.bazaarvoice.megabus.guice.RefResolverConsumerGroup;
import com.bazaarvoice.megabus.guice.RetryRefTopic;
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
                                       KafkaCluster kafkaCluster, Clock clock,
                                       @SelfHostAndPort HostAndPort hostAndPort,
                                       @RefResolverConsumerGroup String refResolverConsumerGroup,
                                       MetricRegistry metricRegistry) {
        super(SERVICE_NAME,
                () -> new MegabusRefResolver(dataProvider, megabusRefTopic, megabusResolvedTopic, retryRefTopic,
                        missingRefTopic, kafkaCluster, clock, hostAndPort, refResolverConsumerGroup, metricRegistry),
                RESTART_DELAY, false);
    }
}
