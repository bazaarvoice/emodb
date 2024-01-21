package com.bazaarvoice.emodb.web.auth.service.serviceimpl;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.DecryptionFailureException;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.InternalServiceErrorException;
import com.amazonaws.services.secretsmanager.model.InvalidParameterException;
import com.amazonaws.services.secretsmanager.model.InvalidRequestException;
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException;
import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.emodb.web.auth.service.SecretsManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.bazaarvoice.emodb.web.auth.service.serviceimpl.ApplicationConstants.MISSING_OR_INVALID_AWS_VALUES;

public class SecretsManagerImpl implements SecretsManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(SecretsManagerImpl.class);
    private AWSSecretsManager client;
    @Inject
    public SecretsManagerImpl(EmoConfiguration configuration)
            throws AwsValuesMissingOrInvalidException {
        client = AWSSecretsManagerClientBuilder
                .standard()
            .withRegion(Regions.fromName(configuration.getAwsConfig().getRegion()))
            .withCredentials(new ProfileCredentialsProvider(configuration.getAwsConfig().getProfile()))
                .build();
    }

    public String getEmodbAuthKeys(String secretName, String secretId) throws AwsValuesMissingOrInvalidException, JsonProcessingException {
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
        return new ObjectMapper().readTree(secret).get(secretId).asText();
    }

}
