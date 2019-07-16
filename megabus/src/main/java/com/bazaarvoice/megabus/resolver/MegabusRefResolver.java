package com.bazaarvoice.megabus.resolver;

import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.kafka.JsonPOJOSerde;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.kafka.metrics.DropwizardMetricsReporter;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.delta.eval.Intrinsics;
import com.bazaarvoice.megabus.MegabusApplicationId;
import com.bazaarvoice.megabus.MegabusRef;
import com.bazaarvoice.megabus.MegabusRefTopic;
import com.bazaarvoice.megabus.MegabusTopic;
import com.bazaarvoice.megabus.MissingRefTopic;
import com.bazaarvoice.megabus.RetryRefTopic;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import static com.google.common.base.Preconditions.checkNotNull;

public class MegabusRefResolver extends AbstractService {

    private final DataProvider _dataProvider;
    private final Topic _megabusRefTopic;
    private final Topic _megabusResolvedTopic;
    private final Topic _retryRefTopic;
    private final Topic _missingRefTopic;
    private final String _applicationId;

    private final KafkaCluster _kafkaCluster;
    private final String _instanceId;
    private final Clock _clock;

    private final Meter _redundantMeter;
    private final Meter _discardedMeter;
    private final Meter _pendingMeter;

    private KafkaStreams _streams;

    @Inject
    public MegabusRefResolver(DataProvider dataProvider, @MegabusRefTopic Topic megabusRefTopic,
                              @MegabusTopic Topic megabusResolvedTopic,
                              @RetryRefTopic Topic retryRefTopic,
                              @MissingRefTopic Topic missingRefTopic,
                              @MegabusApplicationId String applicationId,
                              KafkaCluster kafkaCluster, Clock clock,
                              @SelfHostAndPort HostAndPort hostAndPort,
                              MetricRegistry metricRegistry) {
        _dataProvider = checkNotNull(dataProvider, "dataProvider");
        _megabusRefTopic = checkNotNull(megabusRefTopic, "megabusRefTopic");
        _megabusResolvedTopic = checkNotNull(megabusResolvedTopic, "megabusResolvedTopic");
        _retryRefTopic = checkNotNull(retryRefTopic, "retryRefTopic");
        _missingRefTopic = checkNotNull(missingRefTopic, "missingRefTopic");
        _applicationId = checkNotNull(applicationId, "applicationId");
        _kafkaCluster = checkNotNull(kafkaCluster, "kafkaCluster");
        _instanceId = checkNotNull(hostAndPort).toString();
        _clock = checkNotNull(clock, "clock");

        _redundantMeter = metricRegistry.meter(getMetricName("redundantUpdates"));
        _discardedMeter = metricRegistry.meter(getMetricName("discardedUpdates"));
        _pendingMeter = metricRegistry.meter(getMetricName("pendingUpdates"));

        DropwizardMetricsReporter.registerDefaultMetricsRegistry(metricRegistry);
    }

    private String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.megabus", "MegabusRefResolver", name);
    }

    @Override
    public void doStart() {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, _applicationId  + "-resolver");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaCluster.getBootstrapServers());

        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, _megabusRefTopic.getPartitions());

        streamsConfiguration.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, DropwizardMetricsReporter.class.getName());

        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, _instanceId);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<String, List<MegabusRef>> refStream = streamsBuilder.stream(_megabusRefTopic.getName(), Consumed.with(Serdes.String(), new JsonPOJOSerde<>(new TypeReference<List<MegabusRef>>() {})))
                .merge(streamsBuilder.stream(_retryRefTopic.getName(), Consumed.with(Serdes.String(), new JsonPOJOSerde<>(new TypeReference<List<MegabusRef>>() {}))));

        KStream<String, ResolutionResult> resolutionResults = refStream.mapValues(value -> resolveRefs(value.iterator()));


        resolutionResults.flatMap((key, value) -> value.getKeyedResolvedDocs())
                .to(_megabusResolvedTopic.getName(), Produced.with(Serdes.String(), new JsonPOJOSerde<>(new TypeReference<Map<String, Object>>() {})));

        resolutionResults
                .filterNot((key, result) -> result.getMissingRefs().isEmpty())
                .mapValues(result -> new MissingRefCollection(result.getMissingRefs(), Date.from(_clock.instant())))
                .to(_missingRefTopic.getName(), Produced.with(Serdes.String(), new JsonPOJOSerde<>(MissingRefCollection.class)));

        _streams = new KafkaStreams(streamsBuilder.build(), streamsConfiguration);
        _streams.start();

        notifyStarted();
    }

    private static class ResolutionResult {

        private List<Map<String, Object>> _resolvedDocs;
        private List<MegabusRef> _missingRefs;

        public ResolutionResult(List<Map<String, Object>> resolvedDocs, List<MegabusRef> missingRefs) {
            _resolvedDocs = resolvedDocs;
            _missingRefs = missingRefs;
        }

        public List<Map<String, Object>> getResolvedDocs() {
            return _resolvedDocs;
        }

        public List<MegabusRef> getMissingRefs() {
            return _missingRefs;
        }

        public Iterable<KeyValue<String, Map<String, Object>>> getKeyedResolvedDocs() {
            return Lists.transform(_resolvedDocs, doc -> new KeyValue<>(Coordinate.fromJson(doc).toString(), Intrinsic.isDeleted(doc) ? null : doc));

        }
    }

    private ResolutionResult resolveRefs(Iterator<MegabusRef> refs) {
        Table<Coordinate, UUID, MegabusRef> refTable = HashBasedTable.create();
        refs.forEachRemaining(ref ->
                refTable.put(Coordinate.of(ref.getTable(), ref.getKey()), ref.getChangeId(), ref)
        );

        DataProvider.AnnotatedGet annotatedGet = _dataProvider.prepareGetAnnotated(ReadConsistency.STRONG);

        for (Coordinate coord : refTable.rowKeySet()) {
            try {
                annotatedGet.add(coord.getTable(), coord.getId());
            } catch (UnknownTableException | UnknownPlacementException e) {
                // take no action and discard the event, as the table was deleted before we could process the event
                _discardedMeter.mark();
            }
        }

        Iterator<DataProvider.AnnotatedContent> readResultIter = annotatedGet.execute();

        List<Map<String, Object>> resolvedDocuments = new ArrayList<>();
        List<MegabusRef> missingRefs = new ArrayList<>();

        readResultIter.forEachRemaining(result -> {
            Map<String, Object> content = result.getContent();
            AtomicBoolean triggerEvent = new AtomicBoolean(false);

            refTable.row(Coordinate.fromJson(content)).forEach((changeId, ref) -> {

                if (result.isChangeDeltaPending(changeId)) {
                    _pendingMeter.mark();
                    missingRefs.add(ref);
                    return;
                }

                if (result.isChangeDeltaRedundant(changeId)) {
                    _redundantMeter.mark();
                    return;
                }

                triggerEvent.set(true);
            });

            if (triggerEvent.get()) {
                resolvedDocuments.add(content);
            }
        });

        return new ResolutionResult(resolvedDocuments, missingRefs);
    }


    @Override
    protected void doStop() {
        _streams.close();
        notifyStopped();
    }
}
