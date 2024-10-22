package com.bazaarvoice.emodb.queue.core.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

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
    public Boolean createTopicIfNotExists(String topic, int numPartitions, short replicationFactor, String queueType) {
        Boolean isExisting =isTopicExists(topic);
        if (! isExisting) {
            //create the topic now
            NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
            try {
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                _log.info("Created topic: {} with numPartitions: {} ", topic, numPartitions, replicationFactor);
            } catch (Exception e) {
                 _log.error("Error creating topic {}: {}", topic, e.getMessage());
                throw new RuntimeException(e);
            }
        }
        return isExisting;
    }


    /**
     *  Determines if a topic already exists in AWS MSK
     *  @param topic   The name of the topic.
     */
    public boolean isTopicExists(String topic) {
        try {
            return adminClient.listTopics().names().get().contains(topic);
        } catch (Exception  e) {
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