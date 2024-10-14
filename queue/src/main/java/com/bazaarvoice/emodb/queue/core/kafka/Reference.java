package com.bazaarvoice.emodb.queue.core.kafka;

import software.amazon.awssdk.services.sso.SsoClient;
import software.amazon.awssdk.services.sso.model.GetRoleCredentialsRequest;
import software.amazon.awssdk.services.sso.model.GetRoleCredentialsResponse;
import software.amazon.msk.auth.iam.IAMLoginModule;

public class Reference {

    public static void main(String[] args) {
        // Create an SSO client (dummy usage)
        SsoClient ssoClient = SsoClient.create();

        // Dummy request object
        GetRoleCredentialsRequest request = GetRoleCredentialsRequest.builder()
                .roleName("dummy-role")
                .build();

        // Dummy API call (doesn't actually do anything in this context)
        GetRoleCredentialsResponse response = ssoClient.getRoleCredentials(request);
        IAMLoginModule loginModule = new IAMLoginModule();

        // Print out the response (just to avoid 'unused variable' warnings)
        System.out.println("Dummy SSO Response: " + response);
        ssoClient.close();
    }
}
