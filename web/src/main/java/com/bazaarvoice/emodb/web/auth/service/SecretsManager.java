package com.bazaarvoice.emodb.web.auth.service;

import com.bazaarvoice.emodb.web.auth.serviceimpl.AwsValuesMissingOrInvalidException;
import com.fasterxml.jackson.core.JsonProcessingException;
public interface SecretsManager {
    String getAdminApiKeyAuthToken(String secretName, String secretId) throws AwsValuesMissingOrInvalidException, JsonProcessingException;
    String getReplicationApiKeyApiKeyAuthToken(String secretName, String secretId) throws AwsValuesMissingOrInvalidException, JsonProcessingException;
    String getCompControlApiKeyApiKeyAuthToken(String secretName, String secretId) throws AwsValuesMissingOrInvalidException, JsonProcessingException;
}
