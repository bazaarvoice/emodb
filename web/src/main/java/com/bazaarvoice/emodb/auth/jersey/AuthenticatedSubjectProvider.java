package com.bazaarvoice.emodb.auth.jersey;

import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.server.internal.inject.AbstractValueFactoryProvider;
import org.glassfish.jersey.server.internal.inject.MultivaluedParameterExtractorProvider;
import org.glassfish.jersey.server.model.Parameter;

import javax.inject.Inject;
import javax.ws.rs.ext.Provider;

import static java.util.Objects.requireNonNull;

/**
 * Provider to inject a {@link Subject} as a parameter to a request that requires authentication or authorization.
 * The subject can then be used to perform finer resource restrictions then can be performed through annotations alone.
 */
@Provider
public class AuthenticatedSubjectProvider extends AbstractValueFactoryProvider  {

    private final AuthenticatedSubjectFactory _authenticatedSubjectFactory;

    @Inject
    public AuthenticatedSubjectProvider(MultivaluedParameterExtractorProvider mpep, ServiceLocator locator,
                                        AuthenticatedSubjectFactory authenticatedSubjectFactory) {
        super(mpep, locator, Parameter.Source.UNKNOWN);
        _authenticatedSubjectFactory = requireNonNull(authenticatedSubjectFactory);
    }

    protected Factory<?> createValueFactory(Parameter parameter) {
        Class<?> paramType = parameter.getRawType();
        Authenticated annotation = parameter.getAnnotation(Authenticated.class);
        if (annotation != null && paramType.isAssignableFrom(Subject.class)) {
            return _authenticatedSubjectFactory;
        }

        return null;
    }
}

