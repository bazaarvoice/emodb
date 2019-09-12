package com.bazaarvoice.megabus.resolver;

import com.bazaarvoice.emodb.kafka.JsonPOJOSerde;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.megabus.MegabusRef;
import com.bazaarvoice.megabus.service.KafkaStreamsService;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.net.HostAndPort;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.util.Objects.requireNonNull;

public class MissingRefDelayProcessor extends KafkaStreamsService {

    private static Logger _log = LoggerFactory.getLogger(MissingRefDelayProcessor.class);
    private static final String SERVICE_NAME = "retry";

    private final Topic _retryRefTopic;
    private final Topic _missingRefTopic;
    private Clock _clock;


    public MissingRefDelayProcessor(DataProvider dataProvider,
                                    Topic retryRefTopic,
                                    Topic missingRefTopic,
                                    KafkaCluster kafkaCluster, Clock clock,
                                    HostAndPort hostAndPort,
                                    String delayProcessorConsumerGroup,
                                    MetricRegistry metricRegistry) {
        super(SERVICE_NAME, kafkaCluster.getBootstrapServers(), hostAndPort.toString(),
                delayProcessorConsumerGroup, 1, metricRegistry);

        _retryRefTopic = requireNonNull(retryRefTopic, "retryRefTopic");
        _missingRefTopic = requireNonNull(missingRefTopic, "missingRefTopic");
        _clock = requireNonNull(clock, "clock");
    }

    @Override
    protected Topology topology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.stream(_missingRefTopic.getName(), Consumed.with(Serdes.String(), new JsonPOJOSerde<>(MissingRefCollection.class)))
                .peek(this::delayRefs)
                .mapValues(MissingRefCollection::getMissingRefs)
                .to(_retryRefTopic.getName(), Produced.with(Serdes.String(), new JsonPOJOSerde<>(new TypeReference<List<MegabusRef>>() {})));
        return streamsBuilder.build();

    }



    private void delayRefs(String key, MissingRefCollection refCollection) {
        Instant sendtime = refCollection.getLastProcessTime().toInstant().plusSeconds(10);
        Instant now = _clock.instant();
        if (now.isBefore(sendtime)) {
            try {
                Thread.sleep(sendtime.toEpochMilli() - now.toEpochMilli());
            } catch (InterruptedException e) {
                _log.warn("Attempted sleep during retry failed. Letting message to proceed without required delay.", e);
            }
        }
    }
}
