package com.bazaarvoice.emodb.queue.core.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaConsumerService {
    private static final Logger _log = LoggerFactory.getLogger(KafkaConfig.class);
    private final KafkaConsumer<String, String> consumer; // Changed to String

    public KafkaConsumerService() {
        this.consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProps());
        _log.info("KafkaConsumerService initialized with consumer properties: {}", KafkaConfig.getConsumerProps());
    }

    /**
     * List all the messages count  for each topic
     */
    public void listTopicData () {
        KafkaAdminService kafkaAdminService = new KafkaAdminService();
        Set<String> topicNames = kafkaAdminService.listAllTopics();

        for (String topic : topicNames) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            List<TopicPartition> topicPartitions = partitions.stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .collect(Collectors.toList());

            // Get earliest and latest offsets
            consumer.assign(topicPartitions);
            Map<TopicPartition, Long> startOffsets = consumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

            // Calculate total messages in each topic
            long totalMessages = 0;
            for (TopicPartition tp : topicPartitions) {
                long startOffset = startOffsets.get(tp);
                long endOffset = endOffsets.get(tp);
                totalMessages += (endOffset - startOffset);
            }

            _log.info("Topic: {}, Messages: {}", topic, totalMessages);
        }
    }

    /**
     * Closes the Kafka consumer to release resources.
     */
    public void close() {
        _log.info("Closing Kafka consumer");
        consumer.close();
    }
}