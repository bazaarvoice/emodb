package com.bazaarvoice.emodb.web.auth.jersey;

import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.google.common.collect.ImmutableMap;
import org.apache.shiro.authc.AuthenticationException;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.Providers;

/**
 * Exception mapper for authentication errors.
 */
@Provider
public class AuthenticationExceptionHandler implements ExceptionMapper<AuthenticationException> {

    private final Providers _providers;

    public AuthenticationExceptionHandler(@Context Providers providers) {
        _providers = providers;
    }

    @Override
    public Response toResponse(AuthenticationException exception) {
        // AuthenticationException is only used internally to propagate authorization errors.  Convert the
        // exception to the equivalent public-facing exception from the API.
        UnauthorizedException apiException = new UnauthorizedException();
        return _providers.getExceptionMapper(UnauthorizedException.class).toResponse(apiException);
    }
}
