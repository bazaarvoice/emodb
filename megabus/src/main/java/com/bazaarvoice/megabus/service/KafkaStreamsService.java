package com.bazaarvoice.megabus.service;

import com.bazaarvoice.emodb.kafka.Constants;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.SslConfiguration;
import com.bazaarvoice.emodb.kafka.metrics.DropwizardMetricsReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AbstractService;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public abstract class KafkaStreamsService extends AbstractService implements KafkaStreams.StateListener {

    private final Properties _streamsConfiguration;
    private final AtomicReference<Throwable> _uncaughtException;
    private final AtomicBoolean _fatalErrorEncountered;
    private final Meter _streamsExceptionMeter;

    private KafkaStreams _streams;

    public KafkaStreamsService(String serviceName,
                               KafkaCluster kafkaCluster,
                               String instanceId,
                               String consumerGroupName,
                               int streamThreads,
                               MetricRegistry metricRegistry) {
        _uncaughtException = new AtomicReference<>();
        _fatalErrorEncountered = new AtomicBoolean(false);

        DropwizardMetricsReporter.registerDefaultMetricsRegistry(metricRegistry);

        _streamsConfiguration = new Properties();

        _streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, consumerGroupName);

        _streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());

        _streamsConfiguration.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, DropwizardMetricsReporter.class.getName());

        _streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads);

        _streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), Constants.ACKS_CONFIG);

        _streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), Constants.PRODUCER_COMPRESSION_TYPE);

        _streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), Constants.MAX_REQUEST_SIZE);

        _streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, instanceId + "-" + serviceName);

        SslConfiguration sslConfiguration = kafkaCluster.getSSLConfiguration();
        if (null != sslConfiguration) {
            _streamsConfiguration.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SslConfiguration.PROTOCOL);

            _streamsConfiguration.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslConfiguration.getTrustStoreLocation());
            _streamsConfiguration.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslConfiguration.getTrustStorePassword());

            _streamsConfiguration.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslConfiguration.getKeyStoreLocation());
            _streamsConfiguration.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslConfiguration.getKeyStorePassword());
            _streamsConfiguration.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslConfiguration.getKeyPassword());
        }

        _streamsExceptionMeter = metricRegistry.meter("bv.emodb.megabus.kafka-streams-exceptions");
    }

    protected abstract Topology topology();

    @Override
    protected final void doStart() {
        _streams = new KafkaStreams(topology(), _streamsConfiguration);
        _streams.setUncaughtExceptionHandler((thread, throwable) -> {
            _uncaughtException.compareAndSet(null, throwable);
            _fatalErrorEncountered.set(true);
            _streamsExceptionMeter.mark();
            _streams.close(Duration.ofMillis(1));
        });
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
