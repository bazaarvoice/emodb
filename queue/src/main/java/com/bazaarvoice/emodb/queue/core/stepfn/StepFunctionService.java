package com.bazaarvoice.emodb.queue.core.stepfn;


import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.model.*;
import com.bazaarvoice.emodb.queue.core.Entities.QueueExecutionAttributes;
import com.bazaarvoice.emodb.queue.core.ssm.ParameterStoreUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
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

    private void validateExecutionInputs(String queueType, String queueName, QueueExecutionAttributes executionAttributes) {
        if(queueName == null || queueName.isEmpty()) {
            throw new IllegalArgumentException("queue name can't be null/empty");
        }

        if(queueType == null || queueType.isEmpty()) {
            throw new IllegalArgumentException("queue type can't be null/empty");
        }

        if(!queueType.equals("QUEUE") && !queueType.equals("DEDUP")) {
            throw new IllegalArgumentException("Illegal queue type provided: " + queueType);
        }

        if(executionAttributes == null) {
            throw new IllegalArgumentException("execution attributes can't be null");
        }

        if(executionAttributes.getInterval() == null || executionAttributes.getInterval().isEmpty()) {
            throw new IllegalArgumentException("interval can't be null/empty");
        }

        if(executionAttributes.getBatchSize() == null || executionAttributes.getBatchSize().isEmpty()) {
            throw new IllegalArgumentException("batch size can't be null/empty");
        }

        if(executionAttributes.getQueueThreshold() == null || executionAttributes.getQueueThreshold().isEmpty()) {
            throw new IllegalArgumentException("queue threshold can't be null/empty");
        }
    }

    private String constructPayload(String queueName, String queueType, QueueExecutionAttributes executionAttributes) throws JsonProcessingException {

        validateExecutionInputs(queueName, queueType, executionAttributes);

        executionAttributes.setQueueType(queueType);
        executionAttributes.setQueueName(queueName);

        //TODO_SHAN : Sync up on this topic_name builder and correct it
        if("DEDUP".equals(queueType)) {
            executionAttributes.setTopicName("dnq_dedup" + queueName + "_" + System.currentTimeMillis());
        } else {
            executionAttributes.setTopicName("dnq_" + queueName + "_" + System.currentTimeMillis());
        }

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(executionAttributes);
    }

    public void startExecution(String queueType, String queueName, QueueExecutionAttributes executionAttributes) throws JsonProcessingException {

        if(executionAttributes.getStatus() == null || executionAttributes.getStatus().isEmpty()) {
            throw new IllegalArgumentException("status can't be null/empty");
        }

        if(executionAttributes.getStatus().equals("DISABLED")) {
            throw new IllegalArgumentException("step-function can't be started with status=DISABLED");
        }

        String payload = constructPayload(queueName, queueType, executionAttributes);

        try {
            String stateMachineArn = getStateMachineARN(queueType, queueName);
            StartExecutionRequest startExecutionRequest = new StartExecutionRequest().withStateMachineArn(stateMachineArn)
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
            // TODO_SHAN: Extend this fetch part later based on queueType : queue/dedup/databus
            // TODO_SHAN: String universe = KafkaConfig::getUniverseFromEnv()  // add universe below
            String stateMachineArn = _parameterStoreUtil.getParameter("/emodb/stepfn/stateMachineArn");

            if(stateMachineArn != null && !stateMachineArn.isEmpty()) {
                return stateMachineArn;
            }
        } catch (Exception e) {
            throw new AWSStepFunctionsException("Problem fetching state machine arn for queueType :" + queueType + ", queueName: " + queueName);
        }

        throw new NullPointerException("state machine arn can not be null/empty");

    }

    public void startSFNWithAttributes(QueueExecutionAttributes queueExecutionAttributes) {
        QueueExecutionAttributes existingAttributes;

        //1. fetch attributes for any existing execution
        try {
            existingAttributes = getExistingSFNAttributes(queueExecutionAttributes.getQueueType(), queueExecutionAttributes.getQueueName());
        } catch (Exception e) {
            logger.error("Error getting existing step-function attributes for " + queueExecutionAttributes.toString());
            throw new RuntimeException("Error getting existing step-function attributes for " + queueExecutionAttributes);
        }

        //2. if no running execution exists, start a new one with provided/new attributes
        if (existingAttributes == null) {
            try {
                startExecution(queueExecutionAttributes.getQueueType(), queueExecutionAttributes.getQueueName(), queueExecutionAttributes);
                return;
            } catch(Exception e){
                logger.error("Error starting step-function with attributes " + queueExecutionAttributes);
                throw new RuntimeException("Error starting step-function with attributes " + queueExecutionAttributes);
            }
        }

        try {
            stopActiveExecutions(queueExecutionAttributes.getQueueType(), queueExecutionAttributes.getQueueName());
        } catch(Exception e){
            logger.error("Error stopping step-function for queueName: " + queueExecutionAttributes.getQueueName() + ", queueType: " + queueExecutionAttributes.getQueueType());
            throw new RuntimeException("Error stopping step-function for queueName: " + queueExecutionAttributes.getQueueName() + ", queueType: " + queueExecutionAttributes.getQueueType());
        }

        //3.1 if new attributes can't start a fresh execution, re-start the already running sfn
        //3.2 else start a fresh execution with new attributes
        syncFreshAttributesFromExistingExecution(queueExecutionAttributes, existingAttributes);
        try {
            startExecution(queueExecutionAttributes.getQueueType(), queueExecutionAttributes.getQueueName(), existingAttributes);
        } catch(Exception e){
            logger.error("Error re-starting step-function with attributes " + queueExecutionAttributes);
            throw new RuntimeException("Error re-starting step-function with attributes " + queueExecutionAttributes);
        }
    }

    private void syncFreshAttributesFromExistingExecution(QueueExecutionAttributes newQueueExecutionAttributes, QueueExecutionAttributes existingExecutionAttributes) {

        validateExecutionInputs(existingExecutionAttributes.getQueueType(), existingExecutionAttributes.getQueueName(), existingExecutionAttributes);

        if(newQueueExecutionAttributes == null) {
            newQueueExecutionAttributes = new QueueExecutionAttributes();
        }

        if(newQueueExecutionAttributes.getQueueType() == null || newQueueExecutionAttributes.getQueueType().isEmpty()) {
            newQueueExecutionAttributes.setQueueType(existingExecutionAttributes.getQueueType());
        }

        if(newQueueExecutionAttributes.getQueueName() == null || newQueueExecutionAttributes.getQueueName().isEmpty()) {
            newQueueExecutionAttributes.setQueueName(existingExecutionAttributes.getQueueName());
        }

        if(newQueueExecutionAttributes.getQueueThreshold() == null || newQueueExecutionAttributes.getQueueThreshold().isEmpty()) {
            newQueueExecutionAttributes.setQueueThreshold(existingExecutionAttributes.getQueueThreshold());
        }

        if(newQueueExecutionAttributes.getBatchSize() == null || newQueueExecutionAttributes.getBatchSize().isEmpty()) {
            newQueueExecutionAttributes.setBatchSize(existingExecutionAttributes.getBatchSize());
        }

        if(newQueueExecutionAttributes.getInterval() == null || newQueueExecutionAttributes.getInterval().isEmpty()) {
            newQueueExecutionAttributes.setInterval(existingExecutionAttributes.getInterval());
        }

        if(newQueueExecutionAttributes.getTopicName() == null || newQueueExecutionAttributes.getTopicName().isEmpty()) {
            newQueueExecutionAttributes.setTopicName(existingExecutionAttributes.getTopicName());
        }

        if(newQueueExecutionAttributes.getStatus() == null || newQueueExecutionAttributes.getStatus().isEmpty()) {
            newQueueExecutionAttributes.setStatus(existingExecutionAttributes.getStatus());
        }
    }

}
