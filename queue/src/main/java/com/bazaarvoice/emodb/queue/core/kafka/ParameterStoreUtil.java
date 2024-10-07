package com.bazaarvoice.emodb.queue.core.kafka;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;

public class ParameterStoreUtil {

    private final AWSSimpleSystemsManagement ssmClient;

    public ParameterStoreUtil() {
        // Create SSM client with default credentials and region
        ssmClient = AWSSimpleSystemsManagementClientBuilder.standard()
                .withCredentials( new ProfileCredentialsProvider("emodb-nexus-qa"))
                .build();
    }

    public String getParameter(String parameterName) {
        GetParameterRequest request = new GetParameterRequest().withName(parameterName).withWithDecryption(true);
        GetParameterResult result = ssmClient.getParameter(request);
        return result.getParameter().getValue();
    }
}
