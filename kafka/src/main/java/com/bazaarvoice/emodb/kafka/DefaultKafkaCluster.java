package com.bazaarvoice.emodb.kafka;

import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DefaultKafkaCluster implements KafkaCluster {

    private final AdminClient _adminClient;
    private final String _bootstrapServers;
    private final String _instanceIdentifier;
    private final KafkaProducerConfiguration _kafkaProducerConfiguration;
    private final Supplier<Producer<String, JsonNode>> _producerSupplier;
    private final SslConfiguration _sslConfiguration;

    @Inject
    public DefaultKafkaCluster(AdminClient adminClient,
                               @BootstrapServers String bootstrapServers,
                               @Nullable SslConfiguration sslConfiguration,
                               @SelfHostAndPort HostAndPort hostAndPort,
                               KafkaProducerConfiguration producerConfiguration) {
        _adminClient = requireNonNull(adminClient);
        _bootstrapServers = requireNonNull(bootstrapServers);
        _instanceIdentifier = requireNonNull(hostAndPort).toString();
        _kafkaProducerConfiguration = requireNonNull(producerConfiguration);
        _producerSupplier = Suppliers.memoize(this::createProducer);
        _sslConfiguration = sslConfiguration;
    }

    @Override
    public void createTopicIfNotExists(Topic topic, Map<String, String> config) {
        NewTopic newTopic = new NewTopic(topic.getName(), topic.getPartitions(), topic.getReplicationFactor());
        newTopic.configs(config);
        try {
            _adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause() instanceof TopicExistsException) {
                checkTopicPropertiesMatching(topic);
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private void checkTopicPropertiesMatching(Topic topic) {
        TopicDescription topicDescription = Futures.getUnchecked(
                _adminClient.describeTopics(Collections.singleton(topic.getName())).all()).get(topic.getName());

        checkArgument(topicDescription.partitions().size() == topic.getPartitions());
        topicDescription.partitions().forEach(topicPartitionInfo ->
                checkArgument(topicPartitionInfo.replicas().size() == topic.getReplicationFactor()));
    }

    private Producer<String, JsonNode> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, Constants.ACKS_CONFIG);
        props.put(ProducerConfig.RETRIES_CONFIG, Constants.RETRIES_CONFIG);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Constants.MAX_REQUEST_SIZE);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, Constants.PRODUCER_COMPRESSION_TYPE);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, _instanceIdentifier);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        if (_kafkaProducerConfiguration.getLingerMs().isPresent()) {
            props.put(ProducerConfig.LINGER_MS_CONFIG, _kafkaProducerConfiguration.getLingerMs().get());
        }

        if (_kafkaProducerConfiguration.getBatchsize().isPresent()) {
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, _kafkaProducerConfiguration.getBatchsize().get());
        }

        if (_kafkaProducerConfiguration.getBufferMemory().isPresent()) {
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, _kafkaProducerConfiguration.getBufferMemory().get());
        }

        if (null != _sslConfiguration) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SslConfiguration.PROTOCOL);

            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, _sslConfiguration.getTrustStoreLocation());
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, _sslConfiguration.getTrustStorePassword());

            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, _sslConfiguration.getKeyStoreLocation());
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, _sslConfiguration.getKeyStorePassword());
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, _sslConfiguration.getKeyPassword());
        }

        return new KafkaProducer<>(props);
    }

    public Producer<String, JsonNode> producer() {
        return _producerSupplier.get();
    }

    @Override
    public String getBootstrapServers() {
        return _bootstrapServers;
    }

    @Override
    public SslConfiguration getSSLConfiguration() {
        return _sslConfiguration;
    }
}
