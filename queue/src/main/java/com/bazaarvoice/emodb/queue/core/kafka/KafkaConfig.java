package com.bazaarvoice.emodb.queue.core.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParametersRequest;
import software.amazon.awssdk.services.ssm.model.GetParametersResponse;
import software.amazon.awssdk.services.ssm.model.Parameter;
import software.amazon.awssdk.services.ssm.model.SsmException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class KafkaConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    // Static SSM Client and configuration
    private static final SsmClient ssmClient = SsmClient.builder()
            .credentialsProvider(ProfileCredentialsProvider.create("emodb-nexus-qa"))
            .build();

    private static final String DEFAULT_BOOTSTRAP_SERVERS =
            "b-1.qaemodbpocmsk.q4panq.c10.kafka.us-east-1.amazonaws.com:9092," +
                    "b-2.qaemodbpocmsk.q4panq.c10.kafka.us-east-1.amazonaws.com:9092";

    private static String bootstrapServersConfig;
    private static String batchSizeConfig;
    private static String retriesConfig;
    private static String lingerMsConfig;

    static {
        try {
            // Load configurations from SSM during static initialization
            Map<String, String> parameterValues = getParameterValues(
                    Arrays.asList(
                            "/emodb/kafka/batchSize",
                            "/emodb/kafka/retries",
                            "/emodb/kafka/lingerMs",
                            "/emodb/kafka/bootstrapServers"
                    )
            );

            // Set configurations with fallback to defaults if not present
            batchSizeConfig = parameterValues.getOrDefault("/emodb/kafka/batchSize", "16384");
            retriesConfig = parameterValues.getOrDefault("/emodb/kafka/retries", "3");
            lingerMsConfig = parameterValues.getOrDefault("/emodb/kafka/lingerMs", "1");
            bootstrapServersConfig = parameterValues.getOrDefault("/emodb/kafka/bootstrapServers", DEFAULT_BOOTSTRAP_SERVERS);

            logger.info("Kafka configurations loaded successfully from SSM.");
        } catch (SsmException e) {
            logger.error("Failed to load configurations from SSM. Using default values.", e);
            batchSizeConfig = "16384";
            retriesConfig = "3";
            lingerMsConfig = "1";
            bootstrapServersConfig = DEFAULT_BOOTSTRAP_SERVERS;
        }
    }

    // Fetch parameters from AWS SSM
    private static Map<String, String> getParameterValues(List<String> parameterNames) {
        try {
            GetParametersRequest request = GetParametersRequest.builder()
                    .names(parameterNames)
                    .build();
            GetParametersResponse response = ssmClient.getParameters(request);

            return response.parameters().stream()
                    .collect(Collectors.toMap(Parameter::name, Parameter::value));
        } catch (SsmException e) {
            logger.error("Error fetching parameters from SSM.", e);
            throw e;  // Rethrow or handle the exception if necessary
        }
    }

    // Kafka Producer properties
    public static Properties getProducerProps() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.parseInt(retriesConfig));
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, Integer.parseInt(lingerMsConfig));
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(batchSizeConfig));
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);  // Default buffer memory setting
        logger.info("Kafka Producer properties initialized.");
        return producerProps;
    }

    // Kafka Admin properties
    public static Properties getAdminProps() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        logger.info("Kafka Admin properties initialized.");
        return adminProps;
    }

    // Ensure the SSM client is closed when the application shuts down
    public static void shutdown() {
        if (ssmClient != null) {
            try {
                ssmClient.close();
                logger.info("SSM client closed successfully.");
            } catch (Exception e) {
                logger.error("Error while closing SSM client.", e);
            }
        }
    }
}
