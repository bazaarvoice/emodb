package com.bazaarvoice.emodb.queue.core.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.Future;

public class KafkaProducerService {
    private static final Logger _log = LoggerFactory.getLogger(KafkaProducerService.class);
    private final KafkaProducer<String, String> producer; // Changed to String

    public KafkaProducerService() {
        this.producer = new KafkaProducer<>(KafkaConfig.getProducerProps());
        _log.info("KafkaProducerService initialized with producer properties: {}", KafkaConfig.getProducerProps());
    }

    /**
     * Sends each message from the collection to the specified Kafka topic separately.
     *
     * @param topic   The Kafka topic.
     * @param events  The collection of messages to be sent.
     */
    public <T> void sendMessages(String topic, Collection<T> events, String queueType) {
        _log.info("Sending {} messages to topic '{}'", events.size(), topic);
        for (T event : events) {
            _log.debug("Sending message: {}", event);
            sendMessage(topic, event.toString(),queueType);
        }
        _log.info("Finished sending messages to topic '{}'", topic);
    }

    /**
     * Sends a single message to the specified Kafka topic.
     *
     * @param topic   The Kafka topic.
     * @param message The message to be sent.
     */
    public void sendMessage(String topic, String message, String queueType) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        _log.debug("Preparing to send message to topic '{}' with value: {}", topic, message);

        try {
            Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    _log.error("Failed to send message to topic '{}'. Error: {}", topic, exception.getMessage());
                } else {
                    _log.debug("Message sent to topic '{}' partition {} at offset {}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
            // Optionally, you can wait for the send to complete
            RecordMetadata metadata = future.get(); // Blocking call
            _log.info("Message sent successfully to topic '{}' partition {} at offset {}",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (Exception e) {
            _log.error("Failed to send message to topic '{}'. Exception: {}", topic, e.getMessage());
        }
    }

    /**
     * Closes the producer to release resources.
     */
    public void close() {
        _log.info("Closing Kafka producer.");
        producer.flush();
        producer.close();
        _log.info("Kafka producer closed.");
    }
}