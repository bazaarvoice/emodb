package com.bazaarvoice.emodb.queue.core.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import software.amazon.awssdk.services.ssm.model.GetParameterResponse;
import software.amazon.awssdk.services.ssm.model.ParameterNotFoundException;
import software.amazon.awssdk.services.ssm.model.SsmException;

import java.time.Duration;

/**
 * Utility class for interacting with AWS Parameter Store.
 */
public class ParameterStoreUtil {

    private static final Logger logger = LoggerFactory.getLogger(ParameterStoreUtil.class);
    private final SsmClient ssmClient;

    /**
     * Constructor to initialize the SSM client
     */
    public ParameterStoreUtil() {
        // Create SSM client with specific profile credentials and default region
        this.ssmClient = SsmClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create("emodb-nexus-qa"))
                .build();
    }

    /**
     * Fetches a parameter from AWS Parameter Store.
     *
     * @param parameterName The name of the parameter to fetch
     * @return The value of the parameter
     * @throws IllegalArgumentException If the parameterName is null or empty
     */
    public String getParameter(String parameterName) {
        if (parameterName == null || parameterName.isEmpty()) {
            logger.error("Parameter name cannot be null or empty");
            throw new IllegalArgumentException("Parameter name cannot be null or empty");
        }

        try {
            logger.info("Fetching parameter from AWS Parameter Store: {}", parameterName);

            GetParameterRequest request = GetParameterRequest.builder()
                    .name(parameterName)
                    .build();

            GetParameterResponse response = ssmClient.getParameter(request);

            logger.info("Successfully retrieved parameter: {}", parameterName);
            return response.parameter().value();

        } catch (ParameterNotFoundException e) {
            logger.error("Parameter not found: {}", parameterName, e);
            throw new RuntimeException("Parameter not found: " + parameterName, e);

        } catch (SsmException e) {
            logger.error("Error fetching parameter from AWS SSM: {}", e.getMessage(), e);
            throw new RuntimeException("Error fetching parameter from AWS SSM: " + parameterName, e);

        } catch (Exception e) {
            logger.error("Unexpected error while fetching parameter: {}", parameterName, e);
            throw new RuntimeException("Unexpected error fetching parameter: " + parameterName, e);
        }
    }

    /**
     * Shutdown the SsmClient to release resources.
     */
    public void shutdown() {
        if (ssmClient != null) {
            logger.info("Shutting down SSM Client");
            ssmClient.close();
        }
    }
}
