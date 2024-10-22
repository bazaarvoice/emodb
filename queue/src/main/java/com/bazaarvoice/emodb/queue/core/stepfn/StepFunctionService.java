package com.bazaarvoice.emodb.queue.core.stepfn;


import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.model.*;
import com.bazaarvoice.emodb.queue.core.Entities.QueueExecutionAttributes;
import com.bazaarvoice.emodb.queue.core.ssm.ParameterStoreUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public void startExecution(String queueName, String queueType, QueueExecutionAttributes executionAttributes) throws JsonProcessingException {

        String payload = "{}";
        if(executionAttributes == null) {
            logger.warn("Input payload is null; using empty JSON object");
        } else {
            ObjectMapper objectMapper = new ObjectMapper();
            payload = objectMapper.writeValueAsString(executionAttributes);
        }

        try {
            String stateMachineArn = getStateMachineARN(queueType, queueName);
            StartExecutionRequest startExecutionRequest = new StartExecutionRequest()
                    .withStateMachineArn(stateMachineArn)
                    .withInput(payload);

            StartExecutionResult startExecutionResult = stepFunctionsClient.startExecution(startExecutionRequest);

            logger.info("Successfully started execution for state machine ARN: {}", stateMachineArn);
            logger.debug("Execution ARN: {}", startExecutionResult.getExecutionArn());

        } catch (StateMachineDoesNotExistException e) {
            logger.error("State Machine does not exist for queue_type: " + queueType + ", queue_name: " + queueName, e);
        } catch (InvalidArnException e) {
            logger.error("Invalid ARN provided for queue_type: " + queueType + ", queue_name: " + queueName, e);
        } catch (InvalidExecutionInputException e) {
            logger.error("Invalid execution input provided: {}", payload, e);
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

        try {
            String stateMachineArn = getStateMachineARN(queueType, queueName);

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

    public String getStateMachineARN(String queueType, String queueName) {

        try {
            // TODO: Extend this fetch part later based on queueType : queue/dedup/databus
            // TODO: String universe = KafkaConfig::getUniverseFromEnv()  // add universe below
            String stateMachineArn = _parameterStoreUtil.getParameter("/emodb/stepfn/stateMachineArn");

            if(stateMachineArn != null && !stateMachineArn.isEmpty()) {
                return stateMachineArn;
            }
        } catch (Exception e) {
            throw new AWSStepFunctionsException("Problem fetching state machine arn for queueType :" + queueType + ", queueName: " + queueName);
        }

        throw new NullPointerException("state machine arn can not be null/empty");

    }

}
