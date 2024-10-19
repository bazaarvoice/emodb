package com.bazaarvoice.emodb.queue.core.stepfn;


import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to interact with AWS Step Functions using AWS SDK v1.
 */
public class StepFunctionService {

    private static final Logger logger = LoggerFactory.getLogger(StepFunctionService.class);

    private final AWSStepFunctions stepFunctionsClient;

    /**
     * Constructor to initialize Step Function Client with AWS region and credentials.
     */
    public StepFunctionService() {
        this.stepFunctionsClient = AWSStepFunctionsClientBuilder.standard()
                .build();
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

        }  catch (Exception e) {
            logger.error("Unexpected error occurred during Step Function execution: {}", e.getMessage(), e);
            throw e;
        }
    }
}