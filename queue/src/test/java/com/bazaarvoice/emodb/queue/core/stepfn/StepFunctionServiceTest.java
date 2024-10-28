package com.bazaarvoice.emodb.queue.core.stepfn;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class StepFunctionServiceTest {

    private StepFunctionService stepFunctionService;

    @Mock
    private AWSStepFunctions mockStepFunctionsClient;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        stepFunctionService = new StepFunctionService();

        // Use reflection to set the private field stepFunctionsClient
        Field field = StepFunctionService.class.getDeclaredField("stepFunctionsClient");
        field.setAccessible(true); // Make the private field accessible
        field.set(stepFunctionService, mockStepFunctionsClient); // Inject mock
    }

    @Test
    public void testStartExecution_withValidParameters() {
        // Arrange
        String stateMachineArn = "arn:aws:states:us-east-1:123456789012:stateMachine:exampleStateMachine";
        String inputPayload = "{\"key\":\"value\"}";
        String executionName = "testExecution";

        StartExecutionResult mockResult = new StartExecutionResult()
                .withExecutionArn("arn:aws:states:us-east-1:123456789012:execution:exampleExecution");
        when(mockStepFunctionsClient.startExecution(any(StartExecutionRequest.class))).thenReturn(mockResult);

        // Act
        stepFunctionService.startExecution(stateMachineArn, inputPayload, executionName);

        // Assert
        ArgumentCaptor<StartExecutionRequest> requestCaptor = ArgumentCaptor.forClass(StartExecutionRequest.class);
        verify(mockStepFunctionsClient).startExecution(requestCaptor.capture());

        StartExecutionRequest request = requestCaptor.getValue();
        assertEquals(request.getStateMachineArn(), stateMachineArn);
        assertEquals(request.getInput(), inputPayload);
        assertTrue(request.getName().startsWith("testExecution_"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "State Machine ARN cannot be null or empty")
    public void testStartExecution_withNullStateMachineArn() {
        // Arrange
        String stateMachineArn = null;
        String inputPayload = "{\"key\":\"value\"}";
        String executionName = "testExecution";

        // Act
        stepFunctionService.startExecution(stateMachineArn, inputPayload, executionName);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "State Machine ARN cannot be null or empty")
    public void testStartExecution_withEmptyStateMachineArn() {
        // Arrange
        String stateMachineArn = "";
        String inputPayload = "{\"key\":\"value\"}";
        String executionName = "testExecution";

        // Act
        stepFunctionService.startExecution(stateMachineArn, inputPayload, executionName);
    }

    @Test
    public void testStartExecution_withNullInputPayload() {
        // Arrange
        String stateMachineArn = "arn:aws:states:us-east-1:123456789012:stateMachine:exampleStateMachine";
        String executionName = "testExecution";

        StartExecutionResult mockResult = new StartExecutionResult()
                .withExecutionArn("arn:aws:states:us-east-1:123456789012:execution:exampleExecution");
        when(mockStepFunctionsClient.startExecution(any(StartExecutionRequest.class))).thenReturn(mockResult);

        // Act
        stepFunctionService.startExecution(stateMachineArn, null, executionName);

        // Assert
        ArgumentCaptor<StartExecutionRequest> requestCaptor = ArgumentCaptor.forClass(StartExecutionRequest.class);
        verify(mockStepFunctionsClient).startExecution(requestCaptor.capture());

        StartExecutionRequest request = requestCaptor.getValue();
        assertEquals(request.getStateMachineArn(), stateMachineArn);
        assertEquals(request.getInput(), "{}"); // Default to empty payload
    }

    @Test
    public void testSanitizeExecutionName_withInvalidCharacters() {
        // Arrange
        String invalidExecutionName = "test/execution:name*with?invalid|characters";

        // Act
        String sanitized = stepFunctionService.sanitizeExecutionName(invalidExecutionName);

        // Assert
        assertEquals(sanitized, "test_execution_name_with_invalid_characters");
    }

    @Test
    public void testSanitizeExecutionName_withTooLongName() {
        // Arrange
        String longExecutionName = "ThisIsAVeryLongExecutionNameThatExceedsTheMaximumAllowedLengthOfSixtyNineCharactersAndShouldBeTruncatedAtSomePoint";

        // Act
        String sanitized = stepFunctionService.sanitizeExecutionName(longExecutionName);

        // Assert
        assertTrue(sanitized.length() <= 69);
    }

    // New Test Cases for Edge Cases

    @Test
    public void testSanitizeExecutionName_withValidName() {
        // Arrange
        String validExecutionName = "validExecutionName";

        // Act
        String sanitized = stepFunctionService.sanitizeExecutionName(validExecutionName);

        // Print the output
        System.out.println("Sanitized Execution Name: " + sanitized);

        // Assert
        assertEquals(sanitized, validExecutionName); // Should return the same name
    }

    @Test
    public void testSanitizeExecutionName_withLeadingAndTrailingSpaces() {
        // Arrange
        String executionName = "  executionName  ";

        // Act
        String sanitized = stepFunctionService.sanitizeExecutionName(executionName);

        // Print the output
        System.out.println("Sanitized Execution Name: " + sanitized);

        // Assert
        assertEquals(sanitized, "executionName"); // Should trim spaces
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Execution name cannot contain only invalid characters")
    public void testSanitizeExecutionName_withOnlyInvalidCharacters() {
        // Arrange
        String invalidOnly = "*/?|<>"; // Input with only invalid characters

        stepFunctionService.sanitizeExecutionName(invalidOnly);
    }


    @Test
    public void testSanitizeExecutionName_withMaximumLength() {
        // Arrange
        String maxLengthExecutionName = "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEDHDFHDFHHFCN"; // 69 characters

        // Act
        String sanitized = stepFunctionService.sanitizeExecutionName(maxLengthExecutionName);

        // Print the output
        System.out.println("Sanitized Execution Name: " + sanitized);

        // Assert
        assertEquals(sanitized.length(), 69); // Should be exactly 69 characters
    }

    @Test
    public void testSanitizeExecutionName_withMultipleInvalidCharacters() {
        // Arrange
        String executionName = "test//?invalid//name?with*multiple|invalid:characters";

        // Act
        String sanitized = stepFunctionService.sanitizeExecutionName(executionName);

        // Print the output
        System.out.println("Sanitized Execution Name: " + sanitized);

        // Assert
        assertEquals(sanitized, "test___invalid__name_with_multiple_invalid_characters"); // Should replace all invalid characters
    }
}
