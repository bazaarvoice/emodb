package com.bazaarvoice.emodb.auth.apikey;

import com.google.common.base.Objects;
import org.apache.shiro.authc.AuthenticationToken;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link AuthenticationToken} implementation for ApiKeys.  Since API keys do not have credentials (the presence
 * of a valid ApiKey in the request constitutes authentication) the ID is also used as the credentials.
 */
public class ApiKeyAuthenticationToken implements AuthenticationToken {

    private final String _apiKey;

    public ApiKeyAuthenticationToken(String apiKey) {
        _apiKey = checkNotNull(apiKey, "apiKey");
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
        return Objects.toStringHelper(this).add("apiKey", _apiKey).toString();
    }
}
