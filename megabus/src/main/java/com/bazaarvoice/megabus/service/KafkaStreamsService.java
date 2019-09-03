package com.bazaarvoice.megabus.service;

import com.bazaarvoice.emodb.kafka.metrics.DropwizardMetricsReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AbstractService;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public abstract class KafkaStreamsService extends AbstractService implements KafkaStreams.StateListener {

    // 15 MB
    private static final int MAX_MESSAGE_SIZE = 15 * 1024 * 1024;

    private final Properties _streamsConfiguration;
    private final AtomicReference<Throwable> _uncaughtException;
    private final AtomicBoolean _fatalErrorEncountered;
    private KafkaStreams _streams;

    public KafkaStreamsService(String applicationId,
                               String serviceName,
                               String bootstrapServers,
                               String instanceId,
                               MetricRegistry metricRegistry) {
        _uncaughtException = new AtomicReference<>();
        _fatalErrorEncountered = new AtomicBoolean(false);

        DropwizardMetricsReporter.registerDefaultMetricsRegistry(metricRegistry);

        _streamsConfiguration = new Properties();

        _streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId + "-" + serviceName);

        _streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        _streamsConfiguration.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, DropwizardMetricsReporter.class.getName());

        _streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");

        _streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");

        _streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), MAX_MESSAGE_SIZE);

        _streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, instanceId + "-" + serviceName);
    }

    protected abstract Topology topology();

    @Override
    protected final void doStart() {
        _streams = new KafkaStreams(topology(), _streamsConfiguration);
        _streams.setUncaughtExceptionHandler((thread, throwable) -> _uncaughtException.compareAndSet(null, throwable));
        _streams.setStateListener(this);
        _streams.start();
        notifyStarted();
    }

    @Override
    protected final void doStop() {
        _streams.close();
        notifyStopped();
    }

    @Override
    public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
        if (newState == KafkaStreams.State.ERROR) {
            _fatalErrorEncountered.set(true);
            _streams.close(Duration.ofMillis(1));
        } else if (newState == KafkaStreams.State.NOT_RUNNING && _fatalErrorEncountered.get()) {
            notifyFailed(_uncaughtException.get());
        }
    }

}
