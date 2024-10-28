package com.bazaarvoice.emodb.queue.core.kafka;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.AWSSimpleSystemsManagementException;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersResult;
import com.amazonaws.services.simplesystemsmanagement.model.Parameter;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class KafkaConfig {
    private static String bootstrapServersConfig;
    private static String batchSizeConfig;
    private static String retriesConfig;
    private static String lingerMsConfig;
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);
    // Static SSM Client and configuration using AWS SDK v1
    private static final AWSSimpleSystemsManagement ssmClient = AWSSimpleSystemsManagementClientBuilder
            .standard()
            .build();


    static {
        try {
            final String UNIVERSE = getUniverseFromEnv();
            // Load configurations from SSM during static initialization
            Map<String, String> parameterValues = getParameterValues(
                    Arrays.asList(
                            "/" + UNIVERSE + "/emodb/kafka/batchSize",
                            "/" + UNIVERSE + "/emodb/kafka/retries",
                            "/" + UNIVERSE + "/emodb/kafka/lingerMs",
                            "/" + UNIVERSE + "/emodb/kafka/bootstrapServers"
                    )
            );

            // Set configurations with fallback to defaults if not present
            // Sets the batch size for Kafka producer, which controls the amount of data to batch before sending.
            batchSizeConfig = parameterValues.getOrDefault("/" + UNIVERSE + "/emodb/kafka/batchSize", "16384");

            // Sets the number of retry attempts for failed Kafka message sends.
            retriesConfig = parameterValues.getOrDefault("/" + UNIVERSE + "/emodb/kafka/retries", "3");

            // Sets the number of milliseconds a producer is willing to wait before sending a batch out
            lingerMsConfig = parameterValues.getOrDefault("/" + UNIVERSE + "/emodb/kafka/lingerMs", "1");

            // Configures the Kafka broker addresses for producer connections.
            bootstrapServersConfig = parameterValues.get("/" + UNIVERSE + "/emodb/kafka/bootstrapServers");

            logger.info("Kafka configurations loaded successfully from SSM.");
        } catch (AmazonServiceException e) {
            logger.error("Failed to load configurations from SSM. Using default values.", e);
            throw e;
        }
        catch (Exception e) {
            logger.error("Unexpected error occurred while loading configurations from SSM. Using default values.", e);
            throw e;
        }
    }

    public static String getUniverseFromEnv() {
        String filePath = "/etc/environment";
        logger.info("Reading environment file: " + filePath);
        Properties environmentProps = new Properties();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Skip empty lines or comments
                if (line.trim().isEmpty() || line.trim().startsWith("#")) {
                    continue;
                }
                // Split the line into key-value pair
                String[] parts = line.split("=", 2);
                logger.info("parts: " + Arrays.toString(parts));
                if (parts.length == 2) {
                    String key = parts[0].trim();
                    String value = parts[1].trim();
                    // Remove any surrounding quotes from value
                    value = value.replace("\"", "");
                    environmentProps.put(key, value);
                }
            }
            // Access the environment variables
            return environmentProps.getProperty("UNIVERSE");
        } catch (IOException e) {
            logger.error("Error reading environment file: " + e.getMessage());
            throw new RuntimeException("Error reading environment file: " + e.getMessage());
        }
    }
    // Fetch parameters from AWS SSM using AWS SDK v1
    private static Map<String, String> getParameterValues(List<String> parameterNames) {
        try {
            GetParametersRequest request = new GetParametersRequest()
                    .withNames(parameterNames)
                    .withWithDecryption(true);

            GetParametersResult response = ssmClient.getParameters(request);

            return response.getParameters().stream()
                    .collect(Collectors.toMap(Parameter::getName, Parameter::getValue));
        } catch (AWSSimpleSystemsManagementException e) {
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
                ssmClient.shutdown();
                logger.info("SSM client closed successfully.");
            } catch (Exception e) {
                logger.error("Error while closing SSM client.", e);
            }
        }
    }
}