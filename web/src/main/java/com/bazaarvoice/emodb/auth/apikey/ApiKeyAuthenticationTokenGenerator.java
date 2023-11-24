package com.bazaarvoice.emodb.auth.apikey;

import com.bazaarvoice.emodb.auth.jersey.AuthenticationTokenGenerator;
import com.google.common.base.Strings;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * {@link AuthenticationTokenGenerator} implementation for ApiKeys.  Key can arrive as either a header or query param.
 *
 * @see ApiKeyRequest
 */
public class ApiKeyAuthenticationTokenGenerator implements AuthenticationTokenGenerator<ApiKey> {

    @Override
    public ApiKeyAuthenticationToken createToken(ContainerRequestContext context) {
        String apiKey = context.getHeaders().getFirst(ApiKeyRequest.AUTHENTICATION_HEADER);
        if (Strings.isNullOrEmpty(apiKey)) {
            apiKey = (String) context.getProperty(ApiKeyRequest.AUTHENTICATION_PARAM);
            if (Strings.isNullOrEmpty(apiKey)) {
                return null;
            }
        }

        return new ApiKeyAuthenticationToken(apiKey);
    }
}
