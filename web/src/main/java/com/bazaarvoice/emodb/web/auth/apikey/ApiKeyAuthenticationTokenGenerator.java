package com.bazaarvoice.emodb.web.auth.apikey;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyAuthenticationToken;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.jersey.AuthenticationTokenGenerator;
import com.google.common.base.Strings;
import com.sun.jersey.api.core.HttpRequestContext;

/**
 * {@link AuthenticationTokenGenerator} implementation for ApiKeys.  Key can arrive as either a header or query param.
 *
 * @see ApiKeyRequest
 */
public class ApiKeyAuthenticationTokenGenerator implements AuthenticationTokenGenerator<ApiKey> {

    @Override
    public ApiKeyAuthenticationToken createToken(HttpRequestContext context) {
        String apiKey = context.getHeaderValue(ApiKeyRequest.AUTHENTICATION_HEADER);
        if (Strings.isNullOrEmpty(apiKey)) {
            apiKey = context.getQueryParameters().getFirst(ApiKeyRequest.AUTHENTICATION_PARAM);
            if (Strings.isNullOrEmpty(apiKey)) {
                return null;
            }
        }

        return new ApiKeyAuthenticationToken(apiKey);
    }
}
