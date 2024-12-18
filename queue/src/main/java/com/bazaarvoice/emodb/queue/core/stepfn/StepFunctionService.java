package com.bazaarvoice.emodb.queue.core.stepfn;


import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.model.*;
import com.bazaarvoice.emodb.queue.core.kafka.KafkaConfig;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import com.bazaarvoice.emodb.queue.core.Entities.QueueExecutionAttributes;
import com.bazaarvoice.emodb.queue.core.Entities.ExecutionInputWrapper;
import com.bazaarvoice.emodb.queue.core.ssm.ParameterStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.List;
import java.util.MissingResourceException;

/**
 * Service to interact with AWS Step Functions using AWS SDK v1.
 */
public class StepFunctionService {

    private static final Logger logger = LoggerFactory.getLogger(StepFunctionService.class);

    private final AWSStepFunctions stepFunctionsClient;
    private static String universe;
    private final ParameterStoreUtil _parameterStoreUtil;

    /**
     * Constructor to initialize Step Function Client with AWS region and credentials.
     */
    public StepFunctionService() {
        universe=KafkaConfig.getUniverseFromEnv();
        this._parameterStoreUtil = new ParameterStoreUtil();
        this.stepFunctionsClient = AWSStepFunctionsClientBuilder.standard().build();
    }

    /**
     * Sanitizes the execution name by replacing invalid characters with underscores
     * and truncating if needed.
     */
    public String sanitizeExecutionName(String executionName) {
        if (executionName == null || executionName.isEmpty()) {
            throw new IllegalArgumentException("Execution name cannot be null or empty");
        }
        executionName = executionName.trim();
        // Replace invalid characters with underscores
        String sanitized = executionName.replaceAll("[^a-zA-Z0-9\\-_]", "_");

        // Check if the sanitized name is empty or consists only of underscores
        if (sanitized.isEmpty() || sanitized.replaceAll("_", "").isEmpty()) {
            throw new IllegalArgumentException("Execution name cannot contain only invalid characters");
        }

        // Truncate from the beginning if length exceeds 66 characters
        if (sanitized.length() > 66) {
            sanitized = sanitized.substring(sanitized.length() - 66);
        }

        // Log the updated execution name if it has changed
        if (!sanitized.equals(executionName)) {
            logger.info("Updated execution name: {}", sanitized);
        }
        return sanitized;
    }


    /**
     * Starts the execution of a Step Function with provided execution attributes.
     *
     * @param queueExecutionAttributes execution input attributes
     * @throws RuntimeException If method fails to re-start or start with provided execution input attributes
     *
     * queueType and queueName are mandatory inputs
     * CASE-1 (status = "DISABLED" provided) : active execution if any, stops.
     * CASE-2 (all 4 inputs(qt, bs, i, tn) provided): a new execution is started with these attributes, stopping any active one.
     * CASE-3 (any/all of 4 inputs(qt, bs, i, tn) missing): If any active execution exist, a new execution is started with provided inputs updated, stopping the active one
     * CASE-4 (any/all of 4 inputs(qt, bs, i, tn) missing): If any active execution doesn't exist, Exception occurs, IllegalArgumentException
     */
    public void startSFNWithAttributes(QueueExecutionAttributes queueExecutionAttributes) {
        QueueExecutionAttributes existingAttributes;

        //1. fetch attributes for any existing execution
        try {
            existingAttributes = getExistingSFNAttributes(queueExecutionAttributes.getQueueType(), queueExecutionAttributes.getQueueName());
        } catch (Exception e) {
            logger.error("Error getting existing step-function attributes for " + queueExecutionAttributes + " | " + e.getMessage());
            throw new RuntimeException("Error getting existing step-function attributes for " + queueExecutionAttributes + " | " + e.getMessage());
        }

        //2. if no running execution exists, start a new one with provided/new attributes
        if (existingAttributes == null) {
            try {
                startExecution(queueExecutionAttributes.getQueueType(), queueExecutionAttributes.getQueueName(), queueExecutionAttributes);
                return;
            } catch(Exception e){
                logger.error("Error starting step-function with attributes " + queueExecutionAttributes + " | " + e.getMessage());
                throw new RuntimeException("Error starting step-function with attributes " + queueExecutionAttributes + " | " + e.getMessage());
            }
        }

        //3. check sanity of starting a new execution before stopping the older execution.
        syncFreshAttributesFromExistingExecution(queueExecutionAttributes, existingAttributes);

        //4. stop active execution (if any)
        try {
            stopActiveExecutions(queueExecutionAttributes.getQueueType(), queueExecutionAttributes.getQueueName());
            logger.info("Successfully stopped active execution(if any) for queueName: " + queueExecutionAttributes.getQueueName() + ", queueType: " + queueExecutionAttributes.getQueueType());
        } catch(Exception e){
            logger.error("Error stopping step-function for queueName: " + queueExecutionAttributes.getQueueName() + ", queueType: " + queueExecutionAttributes.getQueueType() + " | " + e.getMessage());
            throw new RuntimeException("Error stopping step-function for queueName: " + queueExecutionAttributes.getQueueName() + ", queueType: " + queueExecutionAttributes.getQueueType() + " | " + e.getMessage());
        }

        //4. if new attributes can't start a fresh execution, re-start the already running sfn, else start a fresh execution with new attributes
        try {
            startExecution(queueExecutionAttributes.getQueueType(), queueExecutionAttributes.getQueueName(), queueExecutionAttributes);
        } catch (Exception e){
            logger.error("Error re-starting step-function with attributes " + queueExecutionAttributes + " | " + e.getMessage());
            throw new RuntimeException("Error re-starting step-function with attributes " + queueExecutionAttributes + "|" + e.getMessage());
        }
    }


    /**
     * Starts an execution of step-function associated with (queueType, queueName), with provided attributes.
     *
     * @param queueType queueType
     * @param queueName queueName
     * @param executionAttributes execution inputs
     *
     */
    public void startExecution(String queueType, String queueName, QueueExecutionAttributes executionAttributes) throws JsonProcessingException {

        if(executionAttributes == null) {
            throw new IllegalArgumentException("execution input object can't be null");
        }

        if(executionAttributes.getStatus() == null || executionAttributes.getStatus().isEmpty()) {
            executionAttributes.setStatus("ENABLED");
        }

        if(executionAttributes.getStatus().equals("DISABLED")) {
            logger.info("step-function's execution can't be triggered because status=DISABLED provided" );
            return;
        }

        String payload = constructPayload(queueName, queueType, executionAttributes);

        try {
            String stateMachineArn = getStateMachineARN();
            String executionName = (queueType.equalsIgnoreCase("dedup") ? "D_" : "") + queueName + "_" + System.currentTimeMillis();
            StartExecutionRequest startExecutionRequest = new StartExecutionRequest().withStateMachineArn(stateMachineArn)
                    .withInput(payload)
                    .withName(executionName);

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


    private String constructPayload(String queueName, String queueType, QueueExecutionAttributes executionAttributes) throws JsonProcessingException {

        validateExecutionInputs(queueName, queueType, executionAttributes);
        executionAttributes.setQueueType(queueType);
        executionAttributes.setQueueName(queueName);

        ExecutionInputWrapper executionInputWrapper = new ExecutionInputWrapper();
        executionInputWrapper.setExecutionInput(executionAttributes);

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(executionInputWrapper);
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


    /**
     * Gets execution inputs of an active/running step-function associated with (queueType, queueName).
     *
     * @param queueType queueType
     * @param queueName queueName
     *
     * @return valid QueueExecutionAttributes : if any active execution exists, else NULL.
     *
     * @throws JsonProcessingException If execution input attributes fails in getting converted to a valid execution payload json
     */
    public QueueExecutionAttributes getExistingSFNAttributes(String queueType, String queueName) throws JsonProcessingException {
        try {
            List<ExecutionListItem> executionItemList = getAllActiveExecutionArns();

            for(ExecutionListItem executionItem : executionItemList) {
                String executionARN = executionItem.getExecutionArn();

                DescribeExecutionRequest describeExecutionRequest = new DescribeExecutionRequest().withExecutionArn(executionARN);
                DescribeExecutionResult describeExecutionResult = stepFunctionsClient.describeExecution(describeExecutionRequest);

                String existingInputPayload = describeExecutionResult.getInput();
                QueueExecutionAttributes queueExecutionAttributes = new ObjectMapper().readValue(existingInputPayload, ExecutionInputWrapper.class).getExecutionInput();

                if(queueExecutionAttributes.getQueueType() != null && queueExecutionAttributes.getQueueType().equals(queueType)
                        && queueExecutionAttributes.getQueueName() != null && queueExecutionAttributes.getQueueName().equals(queueName)) {
                    logger.info("Fetched attributes for executionArn: " + executionARN + " => " + queueExecutionAttributes);
                    return queueExecutionAttributes;
                }
            }

            logger.info("No active executions found for queue_type: " + queueType + ", queue_name: " + queueName + " stateMachineARN: ");
            return null;

        } catch (Exception e) {
            logger.error("Unexpected error in fetching sfn attributes for queue_type: " + queueType + ", queue_name: " + queueName);
            throw e;
        }
    }


    /**
     * Stops an active execution of step-function associated with (queueType, queueName), if any.
     *
     * @param queueType queueType
     * @param queueName queueName
     *
     * @throws Exception: If some glitch happens in stopping.
     */
    public void stopActiveExecutions(String queueType, String queueName) throws JsonProcessingException {

        try {
            List<ExecutionListItem> executionItemList = getAllActiveExecutionArns();

            for(ExecutionListItem executionItem : executionItemList) {
                String executionARN = executionItem.getExecutionArn();

                DescribeExecutionRequest describeExecutionRequest = new DescribeExecutionRequest().withExecutionArn(executionARN);
                DescribeExecutionResult describeExecutionResult = stepFunctionsClient.describeExecution(describeExecutionRequest);

                String existingInputPayload = describeExecutionResult.getInput();
                QueueExecutionAttributes queueExecutionAttributes = new ObjectMapper().readValue(existingInputPayload, ExecutionInputWrapper.class).getExecutionInput();

                if(queueExecutionAttributes.getQueueType() != null && queueExecutionAttributes.getQueueType().equals(queueType)
                        && queueExecutionAttributes.getQueueName() != null && queueExecutionAttributes.getQueueName().equals(queueName)) {
                    logger.info("Stopping active execution: " + executionARN);

                    StopExecutionRequest stopRequest =  new StopExecutionRequest().withExecutionArn(executionARN);
                    stepFunctionsClient.stopExecution(stopRequest);

                    logger.info("Stopped execution: " + executionARN);
                    return;
                }
            }

            logger.info("No active execution arn exists for queue_type:" + queueType + ", queue_name:" + queueName);

        } catch (Exception e) {
            logger.error("Failure in stopping active execution: {}", e.getMessage(), e);
            throw e;
        }
    }


    /**
     * Gets execution ARN of an active/running step-function associated with (queueType, queueName).
     *
     * @return String : if any active execution exists, else NULL.
     */
    public List<ExecutionListItem> getAllActiveExecutionArns() {

        try {
            String stateMachineArn = getStateMachineARN();

            ListExecutionsRequest listExecutionRequest = new ListExecutionsRequest().withStateMachineArn(stateMachineArn)
                    .withStatusFilter(ExecutionStatus.RUNNING);

            ListExecutionsResult listExecutionResults = stepFunctionsClient.listExecutions(listExecutionRequest);
            return listExecutionResults.getExecutions();

        } catch (Exception e) {
            logger.error("Unexpected error: {" + e.getMessage() + "} occurred while fetching all active execution ARNs", e);
            throw e;
        }

    }


    /**
     * Gets stateMachine ARN of a step-function from aws parameter-store.
     *
     * @return String: stateMachineArn
     *
     * @throws AWSStepFunctionsException: If some glitch happens at aws end
     * @throws MissingResourceException: If state machine arn is not found/set in aws parameter store
     */
    public String getStateMachineARN() {

        try {
            // TODO_SHAN: Extend this fetch part later based on queueType : queue/dedup/databus
            String stateMachineArn = _parameterStoreUtil.getParameter("/" + universe + "/emodb/stepfn/stateMachineArn");

            if(stateMachineArn != null && !stateMachineArn.isEmpty()) {
                return stateMachineArn;
            }
        } catch (Exception e) {
            throw new AWSStepFunctionsException("Problem fetching state machine arn");
        }

        throw new MissingResourceException("state machine arn not found in param-store", "", "");
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

        if(newQueueExecutionAttributes.getQueueThreshold() == null) {
            newQueueExecutionAttributes.setQueueThreshold(existingExecutionAttributes.getQueueThreshold());
        }

        if(newQueueExecutionAttributes.getBatchSize() == null) {
            newQueueExecutionAttributes.setBatchSize(existingExecutionAttributes.getBatchSize());
        }

        if(newQueueExecutionAttributes.getInterval() == null) {
            newQueueExecutionAttributes.setInterval(existingExecutionAttributes.getInterval());
        }

        if(newQueueExecutionAttributes.getTopicName() == null || newQueueExecutionAttributes.getTopicName().isEmpty()) {
            newQueueExecutionAttributes.setTopicName(existingExecutionAttributes.getTopicName());
        }

        if(newQueueExecutionAttributes.getStatus() == null || newQueueExecutionAttributes.getStatus().isEmpty()) {
            newQueueExecutionAttributes.setStatus(existingExecutionAttributes.getStatus());
        }

        validateExecutionInputs(newQueueExecutionAttributes.getQueueType(), newQueueExecutionAttributes.getQueueName(), newQueueExecutionAttributes);

    }

    private void validateExecutionInputs(String queueType, String queueName, QueueExecutionAttributes executionAttributes) {
        if(queueName == null || queueName.isEmpty()) {
            throw new IllegalArgumentException("queue name can't be null/empty");
        }

        if(queueType == null || queueType.isEmpty()) {
            throw new IllegalArgumentException("queue type can't be null/empty");
        }

        if(executionAttributes == null) {
            throw new IllegalArgumentException("execution attributes can't be null");
        }

        if(executionAttributes.getTopicName() == null || executionAttributes.getTopicName().isEmpty()) {
            throw new IllegalArgumentException("topic name can't be null/empty");
        }

        if(executionAttributes.getInterval() == null) {
            throw new IllegalArgumentException("interval can't be null");
        }

        if(executionAttributes.getBatchSize() == null) {
            throw new IllegalArgumentException("batch size can't be null");
        }

        if(executionAttributes.getQueueThreshold() == null) {
            throw new IllegalArgumentException("queue threshold can't be null");
        }
    }
}