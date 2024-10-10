package com.bazaarvoice.emodb.queue.core.kafka;


import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import software.amazon.awssdk.services.ssm.model.GetParameterResponse;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaConfig {

    private static final String bootstrapServers = "b-1.qaemodbpocmsk.q4panq.c10.kafka.us-east-1.amazonaws.com:9092,b-2.qaemodbpocmsk.q4panq.c10.kafka.us-east-1.amazonaws.com:9092";

    private static final SsmClient ssmClient = SsmClient.builder()
            .region(Region.US_EAST_1)
            .credentialsProvider(ProfileCredentialsProvider.create("emodb-nexus-qa"))
            .build();
    // Method to fetch parameters from AWS Parameter Store
    private static String getParameterValue(String parameterName) {
        GetParameterRequest request = GetParameterRequest.builder()
                .name(parameterName)
                .build();

        GetParameterResponse result = ssmClient.getParameter(request);
        return result.parameter().value();
    }

    // Fetching configuration properties from Parameter Store
    static String batchSizeConfig = getParameterValue("/emodb/kafka/batchSize");
    static String retriesConfig = getParameterValue("/emodb/kafka/retries");
    static String lingerMsConfig = getParameterValue("/emodb/kafka/lingerMs");
    static String bootstrapServersConfig = getParameterValue("/emodb/kafka/bootstrapServers");

    // Kafka Producer configuration with fetched properties
    public static Properties getProducerProps() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, retriesConfig);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, lingerMsConfig);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSizeConfig);
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        return producerProps;
    }

    public static Properties getAdminProps() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return adminProps;
    }
}
