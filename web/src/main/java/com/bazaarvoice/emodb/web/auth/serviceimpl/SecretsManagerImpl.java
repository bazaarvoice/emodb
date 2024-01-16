package com.bazaarvoice.emodb.web.auth.serviceimpl;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.DecryptionFailureException;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.InternalServiceErrorException;
import com.amazonaws.services.secretsmanager.model.InvalidParameterException;
import com.amazonaws.services.secretsmanager.model.InvalidRequestException;
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException;

import com.bazaarvoice.emodb.web.auth.serviceimpl.AwsValuesMissingOrInvalidException;
import com.bazaarvoice.emodb.web.auth.service.SecretsManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.bazaarvoice.emodb.web.auth.serviceimpl.ApplicationConstants.MISSING_OR_INVALID_AWS_VALUES;

public class SecretsManagerImpl implements SecretsManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SecretsManagerImpl.class);

    private final AWSSecretsManager client;
    private String secretName;
    private String secretId;
    private final ObjectMapper objectMapper;

    @Inject
    public SecretsManagerImpl(AWSSecretsManager client, String secretName,ObjectMapper objectMapper) throws AwsValuesMissingOrInvalidException{
        this.client=client;
        this.secretName=secretName;
        this.objectMapper=objectMapper;
    }

    @Override
    public String getAdminApiKeyAuthToken(String secretName, String secretId) throws AwsValuesMissingOrInvalidException, JsonProcessingException {
        if (StringUtils.isBlank(secretName)){
            throw new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES);
        }
        String secret = null;
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName);
        GetSecretValueResult getSecretValueResult = null;
        try {
            getSecretValueResult = client.getSecretValue(getSecretValueRequest);
        } catch (DecryptionFailureException | InternalServiceErrorException | InvalidParameterException | InvalidRequestException | ResourceNotFoundException e) {
            LOGGER.error("{0}",e);
            throw new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES);
        }
        if (getSecretValueResult.getSecretString() != null) {
            secret = getSecretValueResult.getSecretString();
        }
        Map<String,String> secretMap= new HashMap<>();
        secretMap= objectMapper.readValue(secret,secretMap.getClass());
        return secretMap.get(secretId);
    }

    @Override
    public String getReplicationApiKeyApiKeyAuthToken(String secretName, String secretId) throws AwsValuesMissingOrInvalidException, JsonProcessingException {
        if (StringUtils.isBlank(secretName)){
            throw new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES);
        }
        String secret = null;
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName);
        GetSecretValueResult getSecretValueResult = null;
        try {
            getSecretValueResult = client.getSecretValue(getSecretValueRequest);
        } catch (DecryptionFailureException | InternalServiceErrorException | InvalidParameterException | InvalidRequestException | ResourceNotFoundException e) {
            LOGGER.error("{0}",e);
            throw new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES);
        }
        if (getSecretValueResult.getSecretString() != null) {
            secret = getSecretValueResult.getSecretString();
        }
        Map<String,String> secretMap= new HashMap<>();
        secretMap= objectMapper.readValue(secret,secretMap.getClass());
        return secretMap.get(secretId);
    }

    @Override
    public String getCompControlApiKeyApiKeyAuthToken(String secretName, String secretId) throws AwsValuesMissingOrInvalidException, JsonProcessingException {
        if (StringUtils.isBlank(secretName)){
            throw new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES);
        }
        String secret = null;
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName);
        GetSecretValueResult getSecretValueResult = null;
        try {
            getSecretValueResult = client.getSecretValue(getSecretValueRequest);
        } catch (DecryptionFailureException | InternalServiceErrorException | InvalidParameterException | InvalidRequestException | ResourceNotFoundException e) {
            LOGGER.error("{0}",e);
            throw new AwsValuesMissingOrInvalidException(MISSING_OR_INVALID_AWS_VALUES);
        }
        if (getSecretValueResult.getSecretString() != null) {
            secret = getSecretValueResult.getSecretString();
        }
        Map<String,String> secretMap= new HashMap<>();
        secretMap= objectMapper.readValue(secret,secretMap.getClass());
        return secretMap.get(secretId);
    }


}
