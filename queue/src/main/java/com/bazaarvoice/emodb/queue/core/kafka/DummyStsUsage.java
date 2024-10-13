package com.bazaarvoice.emodb.queue.core.kafka;

import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

public class DummyStsUsage {

    public static void main(String[] args) {
        // Create an STS client (dummy usage)
        StsClient stsClient = StsClient.create();

        // Dummy request to STS to get caller identity
        GetCallerIdentityRequest request = GetCallerIdentityRequest.builder().build();

        // Dummy API call (doesn't actually do anything in this context)
        GetCallerIdentityResponse response = stsClient.getCallerIdentity(request);

        // Print out the response (to avoid unused variable warning)
        System.out.println("Dummy STS Response: " + response);

        // Close the STS client
        stsClient.close();
    }
}
