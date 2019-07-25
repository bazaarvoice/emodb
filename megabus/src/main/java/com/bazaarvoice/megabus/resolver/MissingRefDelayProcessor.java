package com.bazaarvoice.megabus.resolver;

import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.kafka.JsonPOJOSerde;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.kafka.metrics.DropwizardMetricsReporter;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.megabus.MegabusApplicationId;
import com.bazaarvoice.megabus.MegabusRef;
import com.bazaarvoice.megabus.MegabusRefTopic;
import com.bazaarvoice.megabus.MegabusTopic;
import com.bazaarvoice.megabus.MissingRefTopic;
import com.bazaarvoice.megabus.RetryRefTopic;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.google.common.base.Preconditions.checkNotNull;

public class MissingRefDelayProcessor extends AbstractService {

    private static Logger _log = LoggerFactory.getLogger(MissingRefDelayProcessor.class);
    private static final String SUFFIX = "-retry";

    private final Topic _retryRefTopic;
    private final Topic _missingRefTopic;
    private final String _applicationId;

    private KafkaCluster _kafkaCluster;
    private Clock _clock;
    private final String _instanceId;

    private KafkaStreams _streams;

    @Inject
    public MissingRefDelayProcessor(DataProvider dataProvider, @MegabusRefTopic Topic megabusRefTopic,
                                    @MegabusTopic Topic megabusResolvedTopic,
                                    @RetryRefTopic Topic retryRefTopic,
                                    @MissingRefTopic Topic missingRefTopic,
                                    @MegabusApplicationId String applicationId,
                                    KafkaCluster kafkaCluster, Clock clock,
                                    @SelfHostAndPort HostAndPort hostAndPort) {
        _retryRefTopic = checkNotNull(retryRefTopic, "retryRefTopic");
        _missingRefTopic = checkNotNull(missingRefTopic, "missingRefTopic");
        _applicationId = checkNotNull(applicationId, "applicationId");
        _kafkaCluster = checkNotNull(kafkaCluster, "kafkaCluster");
        _instanceId = checkNotNull(hostAndPort).toString();
        _clock = checkNotNull(clock, "clock");
    }

    @Override
    protected void doStart() {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, _applicationId + SUFFIX);
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaCluster.getBootstrapServers());

        streamsConfiguration.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, DropwizardMetricsReporter.class.getName());

        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");

        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, _instanceId + SUFFIX);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.stream(_missingRefTopic.getName(), Consumed.with(Serdes.String(), new JsonPOJOSerde<>(MissingRefCollection.class)))
                .peek(this::delayRefs)
                .mapValues(MissingRefCollection::getMissingRefs)
                .to(_retryRefTopic.getName(), Produced.with(Serdes.String(), new JsonPOJOSerde<>(new TypeReference<List<MegabusRef>>() {})));

        _streams = new KafkaStreams(streamsBuilder.build(), streamsConfiguration);

        _streams.start();

        notifyStarted();
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

    @Override
    protected void doStop() {
        _streams.close();
    }
}
