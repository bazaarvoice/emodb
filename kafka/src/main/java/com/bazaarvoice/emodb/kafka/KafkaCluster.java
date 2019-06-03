package com.bazaarvoice.emodb.kafka;


import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public interface KafkaCluster {

    void createTopicIfNotExists(Topic topic);

    Producer<String, JsonNode> producer();

    String getBootstrapServers();

}
