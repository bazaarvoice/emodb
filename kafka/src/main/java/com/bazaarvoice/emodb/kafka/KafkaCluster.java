package com.bazaarvoice.emodb.kafka;


import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.apache.kafka.clients.producer.Producer;

public interface KafkaCluster {

    void createTopicIfNotExists(Topic topic, Map<String, String> config);

    Producer<String, JsonNode> producer();

    String getBootstrapServers();

}
