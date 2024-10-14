package com.bazaarvoice.emodb.queue.core.stepfn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.SfnException;
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse;
import software.amazon.awssdk.services.sfn.model.StateMachineDoesNotExistException;
import software.amazon.awssdk.services.sfn.model.InvalidArnException;
import software.amazon.awssdk.services.sfn.model.InvalidExecutionInputException;

import java.time.Duration;

/**
 * Production-level service to interact with AWS Step Functions.
 */
public class StepFunctionService {

    private static final Logger logger = LoggerFactory.getLogger(StepFunctionService.class);

    private final SfnClient stepFunctionsClient;

    /**
     * Constructor to initialize Step Function Client with AWS region and credentials.
     */
    public StepFunctionService() {
        this.stepFunctionsClient = SfnClient.builder()
                .region(Region.US_EAST_1)
                .build();
    }

    /**
     * Starts the execution of a Step Function with the given state machine ARN and input payload.
     *
     * @param stateMachineArn ARN of the state machine
     * @param inputPayload    Input for the state machine execution
     * @throws IllegalArgumentException If the stateMachineArn is invalid
     */
    public void startExecution(String stateMachineArn, String inputPayload) {
        if (stateMachineArn == null || stateMachineArn.isEmpty()) {
            logger.error("State Machine ARN cannot be null or empty");
            throw new IllegalArgumentException("State Machine ARN cannot be null or empty");
        }

        if (inputPayload == null) {
            logger.warn("Input payload is null; using empty JSON object");
            inputPayload = "{}"; // Default to empty payload if null
        }

        try {
            StartExecutionRequest startExecutionRequest = StartExecutionRequest.builder()
                    .stateMachineArn(stateMachineArn)
                    .input(inputPayload)
                    .build();

            StartExecutionResponse startExecutionResponse = stepFunctionsClient.startExecution(startExecutionRequest);

            logger.info("Successfully started execution for state machine ARN: {}", stateMachineArn);
            logger.debug("Execution ARN: {}", startExecutionResponse.executionArn());

        } catch (StateMachineDoesNotExistException e) {
            logger.error("State Machine does not exist: {}", stateMachineArn, e);
        } catch (InvalidArnException e) {
            logger.error("Invalid ARN provided: {}", stateMachineArn, e);
        } catch (InvalidExecutionInputException e) {
            logger.error("Invalid execution input provided: {}", inputPayload, e);
        } catch (SfnException e) {
            logger.error("Error executing Step Function: {}", e.getMessage(), e);
            throw e; // Re-throw after logging
        } catch (Exception e) {
            logger.error("Unexpected error occurred during Step Function execution: {}", e.getMessage(), e);
            throw e; // Re-throw unexpected exceptions
        }
    }

    /**
     * Shutdown the SfnClient to release resources.
     */
    public void shutdown() {
        if (stepFunctionsClient != null) {
            logger.info("Shutting down StepFunctionClient");
            stepFunctionsClient.close();
        }
    }
}
