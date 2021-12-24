package com.bazaarvoice.emodb.auth.apikey;

import com.google.common.base.MoreObjects;
import org.apache.shiro.authc.AuthenticationToken;

import static java.util.Objects.requireNonNull;

/**
 * {@link AuthenticationToken} implementation for ApiKeys.  Since API keys do not have credentials (the presence
 * of a valid ApiKey in the request constitutes authentication) the ID is also used as the credentials.
 */
public class ApiKeyAuthenticationToken implements AuthenticationToken {

    private final String _apiKey;

    public ApiKeyAuthenticationToken(String apiKey) {
        _apiKey = requireNonNull(apiKey, "apiKey");
    }

    @Override
    public String getPrincipal() {
        return _apiKey;
    }

    @Override
    public String getCredentials() {
        return _apiKey;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("apiKey", _apiKey).toString();
    }
}
