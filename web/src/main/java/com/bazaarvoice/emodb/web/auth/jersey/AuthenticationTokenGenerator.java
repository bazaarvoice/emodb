package com.bazaarvoice.emodb.web.auth.jersey;

import com.bazaarvoice.emodb.auth.identity.AuthIdentity;
import com.sun.jersey.api.core.HttpRequestContext;
import org.apache.shiro.authc.AuthenticationToken;

/**
 * Interface for creating authorization tokens from the request in a type-safe manner.
 * @param <T>
 */
public interface AuthenticationTokenGenerator<T extends AuthIdentity> {

    /**
     * Returns the authentication token from the caller's request.  If the request did not include sufficient information
     * to generate a token then it returns null.
     * @param context The request context
     * @return The authentication token, or null if one could not be generated
     */
    AuthenticationToken createToken(HttpRequestContext context);
}
