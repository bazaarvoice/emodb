package com.bazaarvoice.emodb.web.auth.service;

import com.bazaarvoice.emodb.web.auth.service.serviceimpl.AwsValuesMissingOrInvalidException;
import com.fasterxml.jackson.core.JsonProcessingException;


public interface SecretsManager {

     public String getEmodbAuthKeys(String secretName, String secretId) throws AwsValuesMissingOrInvalidException, JsonProcessingException;

     }
