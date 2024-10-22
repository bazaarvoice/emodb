package com.bazaarvoice.emodb.queue.core.stepfn;


import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.model.*;
import com.bazaarvoice.emodb.queue.core.ssm.ParameterStoreUtil;
import com.bazaarvoice.emodb.web.Entities.QueueExecutionAttributes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;

/**
 * Production-level service to interact with AWS Step Functions using AWS SDK v1.
 */
public class StepFunctionService {

    private static final Logger logger = LoggerFactory.getLogger(StepFunctionService.class);

    private final AWSStepFunctions stepFunctionsClient;

    private final ParameterStoreUtil _parameterStoreUtil;

    /**
     * Constructor to initialize Step Function Client with AWS region and credentials.
     */
    public StepFunctionService() {
        this._parameterStoreUtil = new ParameterStoreUtil();
        this.stepFunctionsClient = AWSStepFunctionsClientBuilder.standard()
                .withRegion("us-east-1")
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
            StartExecutionRequest startExecutionRequest = new StartExecutionRequest()
                    .withStateMachineArn(stateMachineArn)
                    .withInput(inputPayload);

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

    public void stopActiveExecutions(String queueType, String queueName) {

        try {
            String activeExecutionARN = getActiveExecutionArn(queueType, queueName);
            System.out.println("Stopping active execution: " + activeExecutionARN);

            StopExecutionRequest stopRequest =  new StopExecutionRequest().withExecutionArn(activeExecutionARN);
            stepFunctionsClient.stopExecution(stopRequest);
            System.out.println("Stopped execution: " + activeExecutionARN);
        } catch (Exception e) {
            logger.error("Failure in stopping execution: {}", e.getMessage(), e);
            throw e;
        }
    }

    public QueueExecutionAttributes getExistingSFNAttributes(String queueType, String queueName) throws JsonProcessingException {
        String executionARN;
        try {
            executionARN = getActiveExecutionArn(queueType, queueName);

            if(executionARN != null) {
                logger.info("Fetching details for execution: " + executionARN);
                DescribeExecutionRequest describeExecutionRequest = new DescribeExecutionRequest().withExecutionArn(executionARN);
                DescribeExecutionResult describeExecutionResult = stepFunctionsClient.describeExecution(describeExecutionRequest);

                String existingAttributes = describeExecutionResult.getInput();
                logger.info("Fetched attributes for executionArn: " + executionARN + " => " + existingAttributes);

                return new ObjectMapper().readValue(existingAttributes, QueueExecutionAttributes.class);
            } else {
                logger.info("No active executions found for queue_type: " + queueType + ", queue_name: " + queueName + " stateMachineARN: ");
                return null;
            }

        } catch (Exception e) {
            logger.error("Unexpected error in fetching sfn attributes for queue_type: " + queueType + ", queue_name: " + queueName);
            throw e;
        }
    }

    public String getActiveExecutionArn(String queueType, String queueName) {

        // TODO: Extend this fetch part later based on queueType : queue/dedup/databus
        // TODO: String universe = KafkaConfig::getUniverseFromEnv()  // add universe below
        String stateMachineArn = _parameterStoreUtil.getParameter("/emodb/stepfn/stateMachineArn");   //databus/stateMachineArn

        if(stateMachineArn == null || stateMachineArn.isEmpty()) {
            throw new IllegalArgumentException("state machine arn can not be null/empty");
        }

        try {
            ListExecutionsRequest listExecutionRequest = new ListExecutionsRequest().withStateMachineArn(stateMachineArn)
                    .withStatusFilter(ExecutionStatus.RUNNING);

            ListExecutionsResult listExecutionResults = stepFunctionsClient.listExecutions(listExecutionRequest);

            if (!listExecutionResults.getExecutions().isEmpty()) {
                String executionARN = listExecutionResults.getExecutions().get(0).getExecutionArn();
                logger.info("Fetched executionARN: " + executionARN + " for stateMachineArn: " + stateMachineArn);
                return executionARN;
            } else {
                logger.info("No active executions found for queue_type: " + queueType + ", queue_name: " + queueName + " stateMachineARN: " + stateMachineArn);
                return null;
            }
        } catch (Exception e) {
            logger.error("Unexpected error: {" + e.getMessage() + "} occurred while fetching active execution arn for queue_type: "+ queueType + ", queue_name: " + queueName + " ", e);
            throw e;
        }

    }

}
