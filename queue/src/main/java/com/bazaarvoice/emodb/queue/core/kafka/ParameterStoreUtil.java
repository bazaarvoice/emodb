package com.bazaarvoice.emodb.queue.core.kafka;


import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import software.amazon.awssdk.services.ssm.model.GetParameterResponse;

public class ParameterStoreUtil {

    private final SsmClient ssmClient;

    public ParameterStoreUtil() {
        // Create SSM client with default credentials and region
        ssmClient = SsmClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create("emodb-nexus-qa"))// et your region
                .build();
    }

    public String getParameter(String parameterName) {
        GetParameterRequest request = GetParameterRequest.builder()
                .name(parameterName)
                .withDecryption(true)
                .build();

        GetParameterResponse response = ssmClient.getParameter(request);
        return response.parameter().value();
    }
}
