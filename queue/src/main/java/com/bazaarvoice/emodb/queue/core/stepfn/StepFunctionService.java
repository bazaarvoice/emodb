package com.bazaarvoice.emodb.queue.core.stepfn;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to interact with AWS Step Functions.
 */
public class StepFunctionService {

    private static final Logger logger = LoggerFactory.getLogger(StepFunctionService.class);
    private final SfnClient stepFunctionsClient;

    /**
     * Constructor to initialize Step Function Client with AWS region.
     */
    public StepFunctionService(String region) {
        this.stepFunctionsClient = SfnClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(ProfileCredentialsProvider.create("emodb-nexus-qa"))
                .build();
    }

    /**
     * Starts the execution of a Step Function.
     *
     * @param stateMachineArn ARN of the state machine
     * @param inputPayload Input for the state machine execution
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
        } catch (Exception e) {
            logger.error("Error starting Step Function execution: {}", e.getMessage(), e);
            throw e;
        }
    }
}
