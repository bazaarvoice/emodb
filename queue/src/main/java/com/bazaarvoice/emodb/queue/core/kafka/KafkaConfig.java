package com.bazaarvoice.emodb.queue.core.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaConfig {
    private static String bootstrapServers="b-2.certemodbqueue.7889kt.c3.kafka.us-east-1.amazonaws.com:9092,b-1.certemodbqueue.7889kt.c3.kafka.us-east-1.amazonaws.com:9092";
    public static Properties getProducerProps () {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        return producerProps;
    }

    public static Properties getAdminProps () {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return adminProps;
    }
}

