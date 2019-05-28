package com.bazaarvoice.emodb.kafka;


public interface KafkaCluster {

    void createTopicIfNotExists(Topic topic);

    String getBootstrapServers();

}
