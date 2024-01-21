package com.bazaarvoice.emodb.web.auth.serviceimpl;

import com.bazaarvoice.emodb.web.auth.service.serviceimpl.AwsValuesMissingOrInvalidException;
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

    private final static String validSecretName = "emodb/authkeys";
    private final static String blankSecretName="";
    private final static String invalidSecretName="x4557bmn";
    private static final String MISSING_OR_INVALID_AWS_VALUES ="Missing or invalid aws values";
    private static final String validSecretId ="adminApiKey";
    private static final String invalidSecretId ="cdaaaadminApiKey";
    private final static String validSecretValue="zl+p3AU4/EgT8OtR0ZmLrkL70j0SklugAzd+xxYR1Dz/rioe5aXo4yay7sKi7PSKD59h7/HumH7442nGhlR2rw";

    @BeforeMethod
    public void setup() throws JsonProcessingException {
        initMocks(this);
        when(secretsManager.getEmodbAuthKeys(validSecretName,validSecretId)).thenReturn("zl+p3AU4/EgT8OtR0ZmLrkL70j0SklugAzd+xxYR1Dz/rioe5aXo4yay7sKi7PSKD59h7/HumH7442nGhlR2rw");
        when(secretsManager.getEmodbAuthKeys(blankSecretName,validSecretId)).thenThrow(new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES));
        when(secretsManager.getEmodbAuthKeys(invalidSecretName,validSecretId)).thenThrow(new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES));
        when(secretsManager.getEmodbAuthKeys(validSecretName,invalidSecretId)).thenThrow(new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES));
    }

    @Test
    public void testValidAwsValues() throws JsonProcessingException {
        Assert.assertEquals(secretsManager.getEmodbAuthKeys(validSecretName,validSecretId), validSecretValue);
    }
    @Test(expectedExceptions = AwsValuesMissingOrInvalidException.class,
            expectedExceptionsMessageRegExp = MISSING_OR_INVALID_AWS_VALUES)
    public void testNotValidAwsValues() throws JsonProcessingException {
        secretsManager.getEmodbAuthKeys(invalidSecretName,validSecretId);
        secretsManager.getEmodbAuthKeys(validSecretName,invalidSecretId);
    }

    @Test(expectedExceptions = AwsValuesMissingOrInvalidException.class,
            expectedExceptionsMessageRegExp = MISSING_OR_INVALID_AWS_VALUES)
    public void testisBlankAwsValues() throws JsonProcessingException {
        secretsManager.getEmodbAuthKeys(blankSecretName,validSecretId);
    }

}
