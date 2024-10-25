package com.bazaarvoice.emodb.queue.core.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
    public void sendMessages(String topic, Collection<String> events, String queueType) {
        LocalDateTime startTime = LocalDateTime.now();
        _log.info("Sending {} messages to topic '{}'", events.size(), topic);
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        // Use async sendMessage and collect futures
        for (String event : events) {
            futures.add(producer.send(new ProducerRecord<>(topic, event)));
        }

        // Wait for all futures to complete
        for (Future<RecordMetadata> future : futures) {
            try {
                future.get();  // Only blocks if a future is not yet complete
            } catch (Exception e) {
                _log.error("Error while sending message to Kafka: {}", e.getMessage());
                throw new RuntimeException("Error sending messages to Kafka", e);
            }
        }
        _log.info("Finished sending messages to topic '{}' time taken : {} milliseconds", topic, Duration.between(startTime,LocalDateTime.now()).toMillis());
    }

    /**
     * Sends a single message to the specified Kafka topic.
     *
     * @param topic   The Kafka topic.
     * @param message The message to be sent.
     */
//    public void sendMessage(String topic, String message, String queueType) {
//        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message, message);
//        LocalDateTime startTime = LocalDateTime.now();
//        try {
//            RecordMetadata metadata = producer.send(record).get(); // Blocking call
//            _log.info("Sent One message to {} in {} milliseconds", topic, Duration.between(LocalDateTime.now(),startTime).toMillis());
//        } catch (Exception e) {
//            _log.error("Failed to send message to topic '{}'. Exception: {}", topic, e.getMessage());
//            throw new RuntimeException("Error sending message to kafka"+e.getMessage());
//        }
//    }

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