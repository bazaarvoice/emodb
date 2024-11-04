package com.bazaarvoice.emodb.queue.core.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
        _log.info("Finished sending {} messages to topic '{}'", events.size(), topic);
    }

    /**
     * Sends a single message to the specified Kafka topic.
     *
     * @param topic   The Kafka topic.
     * @param message The message to be sent.
     */
    public void sendMessage(String topic, String message, String queueType) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message, message);
        try {
            RecordMetadata metadata = producer.send(record).get(); // Blocking call
        } catch (Exception e) {
            _log.error("Failed to send message to topic '{}'. Exception: {}", topic, e.getMessage());
            throw new RuntimeException("Error sending message to kafka"+e.getMessage());
        }
    }

    /**
     * Closes the producer to release resources.
     */
    public void close() {
        _log.info("Closing Kafka producer.");
        try {
            producer.flush();
            producer.close();
        } catch (Exception e) {
            _log.error("Error while closing Kafka producer: ", e);
            throw e;
        }
    }
}