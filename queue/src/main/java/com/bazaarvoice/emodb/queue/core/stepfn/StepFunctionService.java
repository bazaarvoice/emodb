package com.bazaarvoice.emodb.queue.core.stepfn;


import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import com.amazonaws.services.stepfunctions.model.StateMachineDoesNotExistException;
import com.amazonaws.services.stepfunctions.model.InvalidArnException;
import com.amazonaws.services.stepfunctions.model.InvalidExecutionInputException;
import com.amazonaws.services.stepfunctions.model.AWSStepFunctionsException;
import com.amazonaws.util.EC2MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Production-level service to interact with AWS Step Functions using AWS SDK v1.
 */
public class StepFunctionService {

    private static final Logger logger = LoggerFactory.getLogger(StepFunctionService.class);

    private final AWSStepFunctions stepFunctionsClient;

    /**
     * Constructor to initialize Step Function Client with AWS region and credentials.
     */
    public StepFunctionService() {
        this.stepFunctionsClient = AWSStepFunctionsClientBuilder.standard()
                .withRegion(provideAmazonRegion().getName())
                .build();
    }
    public static Region provideAmazonRegion() {
        Region reg =  getRegionFromMetadataService().orElse(Region.getRegion(Regions.US_EAST_1));
        logger.info("Region: {}", reg.getName());
        return reg;
    }
    private static Optional<Region> getRegionFromMetadataService() {
        try {
            String region = EC2MetadataUtils.getEC2InstanceRegion(); // SDK method to get region
            logger.info("Region from metadata service: {}", region);
            return Optional.of(Region.getRegion(Regions.fromName(region)));
        } catch (Exception e) {
            logger.error("Error getting region from metadata service: {}", e.getMessage(), e);
            return Optional.empty(); // If we can't determine the region, return empty
        }
    }

    /**
     * Starts the execution of a Step Function with the given state machine ARN and input payload.
     *
     * @param stateMachineArn ARN of the state machine
     * @param inputPayload    Input for the state machine execution
     * @throws IllegalArgumentException If the stateMachineArn is invalid
     */
    public void startExecution(String stateMachineArn, String inputPayload, String executionName) {
        if (stateMachineArn == null || stateMachineArn.isEmpty()) {
            logger.error("State Machine ARN cannot be null or empty");
            throw new IllegalArgumentException("State Machine ARN cannot be null or empty");
        }

        if (inputPayload == null) {
            logger.warn("Input payload is null; using empty JSON object");
            inputPayload = "{}"; // Default to empty payload if null
        }

        try {
            StartExecutionRequest startExecutionRequest = new StartExecutionRequest()
                    .withStateMachineArn(stateMachineArn)
                    .withInput(inputPayload)
                    .withName(executionName);

            StartExecutionResult startExecutionResult = stepFunctionsClient.startExecution(startExecutionRequest);

            logger.info("Successfully started execution for state machine ARN: {}", stateMachineArn);
            logger.debug("Execution ARN: {}", startExecutionResult.getExecutionArn());

        } catch (StateMachineDoesNotExistException e) {
            logger.error("State Machine does not exist: {}", stateMachineArn, e);
        } catch (InvalidArnException e) {
            logger.error("Invalid ARN provided: {}", stateMachineArn, e);
        } catch (InvalidExecutionInputException e) {
            logger.error("Invalid execution input provided: {}", inputPayload, e);
        } catch (AWSStepFunctionsException e) {
            logger.error("Error executing Step Function: {}", e.getMessage(), e);
            throw e; // Re-throw after logging
        } catch (Exception e) {
            logger.error("Unexpected error occurred during Step Function execution: {}", e.getMessage(), e);
            throw e; // Re-throw unexpected exceptions
        }
    }
}