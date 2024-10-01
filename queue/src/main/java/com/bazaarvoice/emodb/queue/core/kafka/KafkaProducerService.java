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
    private final KafkaProducer<String, Collection<String>> producer;

    public KafkaProducerService() {
        this.producer = new KafkaProducer<>(KafkaConfig.getProducerProps());
        _log.info("KafkaProducerService initialized with producer properties: {}", KafkaConfig.getProducerProps());
    }

    /**
     * Sends the entire collection of messages to the specified Kafka topic as a single record.
     *
     * @param topic      The Kafka topic.
     * @param events     The collection of messages to be sent as one message.
     * @param queueType  The type of the queue.
     */
    public void sendMessages(String topic, Collection<String> events, String queueType) {
        _log.info("Sending a collection of {} messages to topic '{}'", events.size(), topic);

        // Sending the entire collection as a single message (one ProducerRecord)
        ProducerRecord<String, Collection<String>> record = new ProducerRecord<>(topic, events);

        try {
            Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    _log.error("Failed to send messages to topic '{}'. Error: {}", topic, exception.getMessage());
                    // Handle exception here or delegate it to other layers (e.g., AbstractQueueService)
                } else {
                    _log.debug("Messages sent to topic '{}' partition {} at offset {}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

            // Optionally, wait for the send to complete (blocking call)
            RecordMetadata metadata = future.get();
            _log.info("Collection of messages sent successfully to topic '{}' partition {} at offset {}",
                    metadata.topic(), metadata.partition(), metadata.offset());

        } catch (Exception e) {
            _log.error("Failed to send collection of messages to topic '{}'. Exception: {}", topic, e.getMessage());
            // Handle exception here or delegate it to other layers (e.g., AbstractQueueService)
        }

        _log.info("Finished sending collection of messages to topic '{}'", topic);
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
