package com.bazaarvoice.emodb.queue.core.ssm;

import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;
import com.amazonaws.services.simplesystemsmanagement.model.PutParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.PutParameterResult;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersResult;
import com.amazonaws.services.simplesystemsmanagement.model.ParameterNotFoundException;
import com.amazonaws.services.simplesystemsmanagement.model.AWSSimpleSystemsManagementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for interacting with AWS Parameter Store using AWS SDK v1.
 */
public class ParameterStoreUtil {

    private static final Logger logger = LoggerFactory.getLogger(ParameterStoreUtil.class);
    private final AWSSimpleSystemsManagement ssmClient;

    /**
     * Constructor to initialize the SSM client
     */
    public ParameterStoreUtil() {
        // Create SSM client with default credentials and region
        ssmClient = AWSSimpleSystemsManagementClientBuilder.standard()
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
            //logger.info("Fetching parameter from AWS Parameter Store: {}", parameterName);

            GetParameterRequest request = new GetParameterRequest().withName(parameterName);
            GetParameterResult result = ssmClient.getParameter(request);

            //logger.info("Successfully retrieved parameter: {}", parameterName);
            return result.getParameter().getValue();

        } catch (ParameterNotFoundException e) {
            logger.error("Parameter not found: {}", parameterName, e);
            throw new RuntimeException("Parameter not found: " + parameterName, e);

        } catch (AWSSimpleSystemsManagementException e) {
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

            GetParametersRequest request = new GetParametersRequest().withNames(parameterNames);
            GetParametersResult result = ssmClient.getParameters(request);

            // Map the result to a Map of parameter names and values
            Map<String, String> parameters = new HashMap<>();
            result.getParameters().forEach(param -> parameters.put(param.getName(), param.getValue()));

            // Log any parameters that were not found
            if (!result.getInvalidParameters().isEmpty()) {
                logger.warn("The following parameters were not found: {}", result.getInvalidParameters());
            }

            logger.info("Successfully retrieved {} parameters", parameters.size());
            return parameters;

        } catch (AWSSimpleSystemsManagementException e) {
            logger.error("Error fetching parameters from AWS SSM: {}", e.getMessage(), e);
            throw new RuntimeException("Error fetching parameters from AWS SSM: " + parameterNames, e);

        } catch (Exception e) {
            logger.error("Unexpected error while fetching parameters: {}", parameterNames, e);
            throw new RuntimeException("Unexpected error fetching parameters: " + parameterNames, e);
        }
    }

    public Long updateParameter(String key, String value) {
        try {
            if (key == null || key.trim().isEmpty()) {
                logger.error("parameter name cannot be null or blank");
                throw new IllegalArgumentException("parameter name cannot be null or blank");
            }

            PutParameterRequest request = new PutParameterRequest().withName(key).withValue(value).withOverwrite(true);

            PutParameterResult response = ssmClient.putParameter(request);
            logger.info("Successfully updated parameter: " + key + " with value: " + value + ", Update Version: " + response.getVersion());
            return response.getVersion();
        } catch (Exception e) {
            logger.error("Failed to update parameter: " + key + " with value: " + value, e);
            throw new RuntimeException("Unexpected error updating parameter: " + key + " with value: " + value, e);
        }
    }

}
