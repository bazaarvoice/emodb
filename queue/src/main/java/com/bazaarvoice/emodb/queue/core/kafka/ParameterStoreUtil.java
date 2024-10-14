package com.bazaarvoice.emodb.queue.core.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
     * Fetches multiple parameters from AWS Parameter Store in a batch.
     *
     * @param parameterNames The list of parameter names to fetch
     * @return A map of parameter names to their values
     * @throws IllegalArgumentException If the parameterNames list is null or empty
     */
    public Map<String, String> getParameters(List<String> parameterNames) {
        if (parameterNames == null || parameterNames.isEmpty()) {
            logger.error("Parameter names list cannot be null or empty");
            throw new IllegalArgumentException("Parameter names list cannot be null or empty");
        }

        try {
            logger.info("Fetching parameters from AWS Parameter Store: {}", parameterNames);

            GetParametersRequest request = GetParametersRequest.builder()
                    .names(parameterNames)
                    .build();

            GetParametersResponse response = ssmClient.getParameters(request);

            // Map the result to a Map of parameter names and values
            Map<String, String> parameters = response.parameters().stream()
                    .collect(Collectors.toMap(p -> p.name(), p -> p.value()));

            // Log any parameters that were not found
            if (!response.invalidParameters().isEmpty()) {
                logger.warn("The following parameters were not found: {}", response.invalidParameters());
            }

            logger.info("Successfully retrieved {} parameters", parameters.size());
            return parameters;

        } catch (SsmException e) {
            logger.error("Error fetching parameters from AWS SSM: {}", e.getMessage(), e);
            throw new RuntimeException("Error fetching parameters from AWS SSM: " + parameterNames, e);

        } catch (Exception e) {
            logger.error("Unexpected error while fetching parameters: {}", parameterNames, e);
            throw new RuntimeException("Unexpected error fetching parameters: " + parameterNames, e);
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
