package com.bazaarvoice.emodb.queue.core.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import java.util.Collections;

public class KafkaAdminService {
    private static final Logger _log = LoggerFactory.getLogger(KafkaAdminService.class);
    private final AdminClient adminClient;
    // Cache for the list of all topics with a TTL of 10 minutes
    private final Cache<String, Set<String>> topicListCache = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .build();

    private static final String TOPIC_LIST_KEY = "allTopics";


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
                addToCache(topic);
                _log.info("Created topic: {} with numPartitions: {} and replication factor {} ", topic, numPartitions, replicationFactor);
            } catch (Exception e) {
                _log.error("Error creating topic {}: ", topic, e);
                throw new RuntimeException(e);
            }
        }
        return isExisting;
    }
    public void addToCache(String topic){
        Set<String> topics = topicListCache.getIfPresent(TOPIC_LIST_KEY);
        if (topics == null) {
            topics = new HashSet<>();
        }
        topics.add(topic);
        topicListCache.put(TOPIC_LIST_KEY, topics);
        _log.info("Added newly created topic to cache: {}", topic);
    }


    /**
     * Checks if a Kafka topic exists by using a cache to store the list of all topics.
     * If the cache entry has expired or the cache is empty, it queries the Kafka AdminClient for the topic list.
     * <p>
     * The cached list has a TTL (Time-To-Live) of 10 minutes, after which it will be refreshed
     * from Kafka on the next access.
     * </p>
     *
     * @param topic the name of the Kafka topic to check
     * @return {@code true} if the topic exists, otherwise {@code false}.
     * @throws RuntimeException if there is an error fetching the topic list or checking if the topic exists.
     */
    public boolean isTopicExists(String topic) {
        try {
            // Retrieve the list of topics from the cache
            Set<String> topics = topicListCache.get(TOPIC_LIST_KEY, this::fetchTopicListFromKafka);

            // Check if the given topic is in the cached list
            return topics.contains(topic);
        } catch (ExecutionException e) {
            _log.error("Failed to check if topic exists: {}", topic, e);
            throw new RuntimeException("Error checking if topic exists", e);
        }
    }

    /**
     * Fetches the list of all topic names from Kafka AdminClient.
     * This method is called only when the cache is expired or empty.
     *
     * @return a Set containing all topic names.
     * @throws ExecutionException if there is an error fetching the topic list from Kafka.
     */
    private Set<String> fetchTopicListFromKafka() throws ExecutionException {
        try {
            _log.info("Fetching topic list from Kafka");
            return adminClient.listTopics().names().get();
        } catch (Exception e) {
            _log.error("Error fetching topic list from Kafka", e);
            throw new ExecutionException(e);
        }
    }

    /**
     * Closes the AdminClient to release resources.
     */
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}