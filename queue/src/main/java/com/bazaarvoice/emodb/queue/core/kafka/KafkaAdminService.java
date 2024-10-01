package com.bazaarvoice.emodb.queue.core.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class KafkaAdminService {
    private static final Logger _log = LoggerFactory.getLogger(KafkaAdminService.class);
    private final AdminClient adminClient;

    public KafkaAdminService() {
        this.adminClient = AdminClient.create(KafkaConfig.getAdminProps());
    }

    /**
     * Creates a new Kafka topic with the specified configurations.
     *
     * @param topic             The name of the topic.
     * @param numPartitions     Number of partitions.
     * @param replicationFactor Replication factor.
     */
    public void createTopic(String topic, int numPartitions, short replicationFactor, String queueType) {
        NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
        try {
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            _log.info("Created topic: {}", topic);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                _log.warn("Topic {} already exists.", topic);
            } else {
                _log.error("Error creating topic {}: {}", topic, e.getMessage());
            }
        } catch (InterruptedException e) {
            _log.error("Interrupted while creating topic {}: {}", topic, e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    /**
     *  Determines if a topic already exists in AWS MSK
     *  @param topic   The name of the topic.
     */
    public boolean isTopicExists(String topic) {
        try {
            return adminClient.listTopics().names().get().contains(topic);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Closes the AdminClient to release resources.
     */
    public void close() {
        adminClient.close();
    }
}
