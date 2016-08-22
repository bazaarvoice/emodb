package com.bazaarvoice.emodb.auth.jersey;

import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import org.apache.shiro.authz.AuthorizationException;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.Providers;

/**
 * Exception mapper for authorization errors.
 */
@Provider
public class AuthorizationExceptionHandler implements ExceptionMapper<AuthorizationException> {

    private final Providers _providers;

    public AuthorizationExceptionHandler(@Context Providers providers) {
        _providers = providers;
    }

    @Override
    public Response toResponse(AuthorizationException exception) {
        // AuthorizationException is only used internally to propagate authorization errors.  Convert the
        // exception to the equivalent public-facing exception from the API.
        UnauthorizedException apiException = new UnauthorizedException("not authorized");
        return _providers.getExceptionMapper(UnauthorizedException.class).toResponse(apiException);
    }
}
