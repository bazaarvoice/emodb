package com.bazaarvoice.emodb.web.auth.serviceimpl;

import com.bazaarvoice.emodb.web.auth.service.SecretsManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class SecretsManagerTest {

    @Mock
    private SecretsManager secretsManager;

    private final static String validSecretName = "qa/emodb/authenticationkeys";
    private final static String validSecretValue="valid-value";
    private final static String blankSecretName="";
    private final static String invalidSecretName="x4557bmn";
    private static final String MISSING_OR_INVALID_AWS_VALUES ="Missing or invalid aws values";
    private static final String validSecretId ="Valid secret id";

    @BeforeMethod
    public void setup() throws JsonProcessingException {
        initMocks(this);
        when(secretsManager.getAdminApiKeyAuthToken(validSecretName,validSecretId)).thenReturn("valid-value");
        when(secretsManager.getAdminApiKeyAuthToken(blankSecretName,validSecretId)).thenThrow(new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES));
        when(secretsManager.getAdminApiKeyAuthToken(invalidSecretName,validSecretId)).thenThrow(new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES));
        when(secretsManager.getReplicationApiKeyApiKeyAuthToken(validSecretName,validSecretId)).thenReturn("valid-value");
        when(secretsManager.getReplicationApiKeyApiKeyAuthToken(blankSecretName,validSecretId)).thenThrow(new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES));
        when(secretsManager.getReplicationApiKeyApiKeyAuthToken(invalidSecretName,validSecretId)).thenThrow(new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES));
        when(secretsManager.getCompControlApiKeyApiKeyAuthToken(validSecretName,validSecretId)).thenReturn("valid-value");
        when(secretsManager.getCompControlApiKeyApiKeyAuthToken(blankSecretName,validSecretId)).thenThrow(new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES));
        when(secretsManager.getCompControlApiKeyApiKeyAuthToken(invalidSecretName,validSecretId)).thenThrow(new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES));
    }

    @Test
    public void testValidAwsValues() throws JsonProcessingException {
        Assert.assertEquals(secretsManager.getAdminApiKeyAuthToken(validSecretName,validSecretId), validSecretValue);
        Assert.assertEquals(secretsManager.getReplicationApiKeyApiKeyAuthToken(validSecretName,validSecretId), validSecretValue);
        Assert.assertEquals(secretsManager.getCompControlApiKeyApiKeyAuthToken(validSecretName,validSecretId), validSecretValue);

    }
    @Test(expectedExceptions = AwsValuesMissingOrInvalidException.class,
            expectedExceptionsMessageRegExp = MISSING_OR_INVALID_AWS_VALUES)
    public void testNotValidAwsValues() throws JsonProcessingException {
        secretsManager.getAdminApiKeyAuthToken(blankSecretName,validSecretId);
        secretsManager.getReplicationApiKeyApiKeyAuthToken(blankSecretName,validSecretId);
        secretsManager.getCompControlApiKeyApiKeyAuthToken(blankSecretName,validSecretId);
    }

    @Test(expectedExceptions = AwsValuesMissingOrInvalidException.class,
            expectedExceptionsMessageRegExp = MISSING_OR_INVALID_AWS_VALUES)
    public void testisBlankAwsValues() throws JsonProcessingException {
        secretsManager.getAdminApiKeyAuthToken(invalidSecretName,validSecretId);
        secretsManager.getReplicationApiKeyApiKeyAuthToken(invalidSecretName,validSecretId);
        secretsManager.getCompControlApiKeyApiKeyAuthToken(invalidSecretName,validSecretId);
    }

}
