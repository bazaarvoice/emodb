package com.bazaarvoice.emodb.kafka;


import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.producer.Producer;

import java.util.Map;

public interface KafkaCluster {

    void createTopicIfNotExists(Topic topic, Map<String, String> config);

    Producer<String, JsonNode> producer();

    String getBootstrapServers();

    SslConfiguration getSSLConfiguration();

}
