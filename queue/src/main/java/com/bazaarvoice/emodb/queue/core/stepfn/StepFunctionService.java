package  com.bazaarvoice.emodb.queue.core.stepfn;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to interact with AWS Step Functions.
 */
public class StepFunctionService {

    private static final Logger logger = LoggerFactory.getLogger(StepFunctionService.class);
    private final AWSStepFunctions stepFunctionsClient;

    /**
     * Constructor to initialize Step Function Client with AWS profile and region.
     */
    public StepFunctionService(String region) {
        this.stepFunctionsClient = AWSStepFunctionsClientBuilder.standard()
                .withRegion(region)
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
            StartExecutionRequest startExecutionRequest = new StartExecutionRequest()
                    .withStateMachineArn(stateMachineArn)
                    .withInput(inputPayload);

            StartExecutionResult startExecutionResult = stepFunctionsClient.startExecution(startExecutionRequest);
            logger.info("Successfully started execution for state machine ARN: {}", stateMachineArn);
            logger.debug("Execution ARN: {}", startExecutionResult.getExecutionArn());
        } catch (Exception e) {
            logger.error("Error starting Step Function execution: {}", e.getMessage(), e);
            throw e;
        }
    }
}
