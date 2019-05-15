package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.bazaarvoice.megabus.streams.JsonPOJOSerde;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import java.util.ArrayList;
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
    private String _bootstrapServers;

    private KafkaStreams _streams;

    @Inject
    public MegabusRefResolver(DataProvider dataProvider, @MegabusRefTopic Topic megabusRefTopic,
                              @BootstrapServers String bootstrapServers) {
        _dataProvider = checkNotNull(dataProvider, "dataProvider");
        _megabusRefTopic = checkNotNull(megabusRefTopic, "megabusRefTopic");
        _bootstrapServers = checkNotNull(bootstrapServers, "bootstrapServers");
    }

    @Override
    public void doStart() {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "megabus-resolver");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapServers);

        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, _megabusRefTopic.getPartitions());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<String, List<UpdateRef>> refStream = streamsBuilder.stream(_megabusRefTopic.getName(), Consumed.with(Serdes.String(), new JsonPOJOSerde<>(new TypeReference<List<UpdateRef>>() {})));

        // for debugging
//        refStream.foreach(((key, value) -> System.out.println(key + " " + value)));

        KStream<String, Map<String, Object>> megabus = refStream.flatMap((key, value) -> resolveRefs(value.iterator()).getKeyedDocs());


        megabus.to("megabus-resolved", Produced.with(Serdes.String(), new JsonPOJOSerde<>(new TypeReference<Map<String, Object>>() {})));

        _streams = new KafkaStreams(streamsBuilder.build(), streamsConfiguration);
        _streams.start();
    }

    private static class ResolutionResult {

        private List<Map<String, Object>> _resolvedDocs;
        private List<UpdateRef> _missingRefs;

        public ResolutionResult(List<Map<String, Object>> resolvedDocs, List<UpdateRef> missingRefs) {
            _resolvedDocs = resolvedDocs;
            _missingRefs = missingRefs;
        }

        public List<Map<String, Object>> getResolvedDocs() {
            return _resolvedDocs;
        }

        public List<UpdateRef> getMissingRefs() {
            return _missingRefs;
        }

        public Iterable<KeyValue<String, Map<String, Object>>> getKeyedDocs() {
            return Lists.transform(_resolvedDocs, doc -> new KeyValue<>(Coordinate.fromJson(doc).toString(), doc));

        }
    }

    private ResolutionResult resolveRefs(Iterator<UpdateRef> refs) {
        Table<Coordinate, UUID, UpdateRef> refTable = HashBasedTable.create();
        refs.forEachRemaining(ref ->
                refTable.put(Coordinate.of(ref.getTable(), ref.getKey()), ref.getChangeId(), ref)
        );

        DataProvider.AnnotatedGet annotatedGet = _dataProvider.prepareGetAnnotated(ReadConsistency.STRONG);

        for (Coordinate coord : refTable.rowKeySet()) {
            try {
                annotatedGet.add(coord.getTable(), coord.getId());
            } catch (UnknownTableException | UnknownPlacementException e) {
                // TODO: handle delete table events gracefully
            }
        }

        Iterator<DataProvider.AnnotatedContent> readResultIter = annotatedGet.execute();

        List<Map<String, Object>> resolvedDocuments = new ArrayList<>();
        List<UpdateRef> missingRefs = new ArrayList<>();

        readResultIter.forEachRemaining(result -> {
            Map<String, Object> content = result.getContent();
            AtomicBoolean triggerEvent = new AtomicBoolean(false);

            refTable.row(Coordinate.fromJson(content)).forEach((changeId, ref) -> {

                if (result.isChangeDeltaPending(changeId)) {
                    missingRefs.add(ref);
                    return;
                }

                if (result.isChangeDeltaRedundant(changeId)) {
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
    }
}
