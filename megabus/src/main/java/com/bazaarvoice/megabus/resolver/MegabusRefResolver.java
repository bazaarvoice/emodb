package com.bazaarvoice.megabus.resolver;

import com.bazaarvoice.emodb.kafka.JsonPOJOSerde;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.megabus.MegabusRef;
import com.bazaarvoice.megabus.service.KafkaStreamsService;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MegabusRefResolver extends KafkaStreamsService {

    private static final Logger _log = LoggerFactory.getLogger(MegabusRefResolver.class);
    private static final String SERVICE_NAME = "resolver";

    private final DataProvider _dataProvider;
    private final Topic _megabusRefTopic;
    private final Topic _megabusResolvedTopic;
    private final Topic _retryRefTopic;
    private final Topic _missingRefTopic;

    private final Clock _clock;

    private final Meter _redundantMeter;
    private final Meter _discardedMeter;
    private final Meter _pendingMeter;
    private final Meter _errorProcessingMeter;
    private final Histogram _processingLatencyHisto;

    @Inject
    public MegabusRefResolver(DataProvider dataProvider, Topic megabusRefTopic,
                              Topic megabusResolvedTopic,
                              Topic retryRefTopic,
                              Topic missingRefTopic,
                              KafkaCluster kafkaCluster, Clock clock,
                              HostAndPort hostAndPort,
                              String refResolverConsumerGroup,
                              MetricRegistry metricRegistry) {
        super(SERVICE_NAME, kafkaCluster, hostAndPort.toString(),
                refResolverConsumerGroup, megabusRefTopic.getPartitions(), metricRegistry);

        _dataProvider = requireNonNull(dataProvider, "dataProvider");
        _megabusRefTopic = requireNonNull(megabusRefTopic, "megabusRefTopic");
        _megabusResolvedTopic = requireNonNull(megabusResolvedTopic, "megabusResolvedTopic");
        _retryRefTopic = requireNonNull(retryRefTopic, "retryRefTopic");
        _missingRefTopic = requireNonNull(missingRefTopic, "missingRefTopic");

        _clock = requireNonNull(clock, "clock");

        _redundantMeter = metricRegistry.meter(getMetricName("redundantUpdates"));
        _discardedMeter = metricRegistry.meter(getMetricName("discardedUpdates"));
        _pendingMeter = metricRegistry.meter(getMetricName("pendingUpdates"));
        _errorProcessingMeter = metricRegistry.meter(getMetricName("errors"));
        _processingLatencyHisto = metricRegistry.histogram(getMetricName("processing-latency-ms"));
    }

    private String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.megabus", "MegabusRefResolver", name);
    }

    @Override
    protected Topology topology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // merge the ref stream with the ref-retry stream. They must be merged into a single stream for ordering purposes
        final KStream<String, List<MegabusRef>> refStream = streamsBuilder.stream(_megabusRefTopic.getName(), Consumed.with(Serdes.String(), new JsonPOJOSerde<>(new TypeReference<List<MegabusRef>>() {})))
                .merge(streamsBuilder.stream(_retryRefTopic.getName(), Consumed.with(Serdes.String(), new JsonPOJOSerde<>(new TypeReference<List<MegabusRef>>() {}))));

        // resolve refs into documents
        KStream<String, ResolutionResult> resolutionResults = refStream.mapValues(value -> {
            try {
                return resolveRefs(value);
            } catch (Throwable t) {
                _errorProcessingMeter.mark();
                throw t;
            }
        });

        resolutionResults
                // extract the resolved documents
                .flatMap((key, value) -> value.getKeyedResolvedDocs())
                // convert deleted documents to null
                .mapValues(doc -> Optional.ofNullable(doc).map(Intrinsic::isDeleted).orElse(true) ? null : doc)
                // send to megabus
                .to(_megabusResolvedTopic.getName(), Produced.with(Serdes.String(), new JsonPOJOSerde<>(new TypeReference<Map<String, Object>>() {})));

        resolutionResults
                // filter out all resolution results without missing refs
                .filterNot((key, result) -> result.getMissingRefs().isEmpty())
                // add timestamp for missing refs
                .mapValues(result -> new MissingRefCollection(result.getMissingRefs(), Date.from(_clock.instant())))
                // send to missing topic
                .to(_missingRefTopic.getName(), Produced.with(Serdes.String(), new JsonPOJOSerde<>(MissingRefCollection.class)));
        return streamsBuilder.build();
    }

    private static class ResolutionResult {

        private Map<Coordinate, Optional<Map<String, Object>>> _resolvedDocs;
        private List<MegabusRef> _missingRefs;

        public ResolutionResult(Map<Coordinate, Optional<Map<String, Object>>> resolvedDocs, List<MegabusRef> missingRefs) {
            _resolvedDocs = resolvedDocs;
            _missingRefs = missingRefs;
        }

        public List<MegabusRef> getMissingRefs() {
            return _missingRefs;
        }

        public Iterable<KeyValue<String, Map<String, Object>>> getKeyedResolvedDocs() {
            return _resolvedDocs.entrySet().stream()
                    .map(entry -> new KeyValue<>(entry.getKey().toString(), entry.getValue().orElse(null)))
                    .collect(Collectors.toList());
        }
    }

    private ResolutionResult resolveRefs(Collection<MegabusRef> refs) {

        // If isDeleted() is true, then this batch was the result of a table event, and we should propogate null's. Additionally,
        // all refs in the batch should have isDeleted() == true. A batch having some refs with true and some with false is an invalid state.
        if (refs.stream().anyMatch(MegabusRef::isDeleted)) {
            Map<Coordinate, Optional<Map<String, Object>>> resolvedRefs = refs.stream()
                    .peek(ref -> checkState(ref.isDeleted()))
                    .collect(Collectors.toMap(ref -> Coordinate.of(ref.getTable(), ref.getKey()), ref -> Optional.empty()));
            return new ResolutionResult(resolvedRefs, Collections.emptyList());
        }

        Table<Coordinate, UUID, MegabusRef> refTable = HashBasedTable.create();
        refs.forEach(ref -> refTable.put(Coordinate.of(ref.getTable(), ref.getKey()), ref.getChangeId(), ref));

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
        Map<Coordinate, Optional<Map<String, Object>>> resolvedDocuments = new HashMap<>();
        List<MegabusRef> missingRefs = new ArrayList<>();

        readResultIter.forEachRemaining(result -> {
            Map<String, Object> content = result.getContent();
            AtomicBoolean triggerEvent = new AtomicBoolean(false);

            Coordinate coordinate = Coordinate.fromJson(content);

            refTable.row(coordinate).forEach((changeId, ref) -> {

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

                final Instant mark = _clock.instant();
                final Instant readAt = ref.getReadTime();
                if (readAt != null) {
                    _log.debug("doc[{}], readAt[{}], age[{}ms]", ref.getKey(), readAt, mark.toEpochMilli() - readAt.toEpochMilli());
                    _processingLatencyHisto.update(mark.toEpochMilli() - readAt.toEpochMilli());
                }
            });

            if (triggerEvent.get()) {
                resolvedDocuments.put(coordinate, Optional.of(content));
            }
        });

        return new ResolutionResult(resolvedDocuments, missingRefs);
    }
}
