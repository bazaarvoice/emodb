package com.bazaarvoice.emodb.auth.jersey;

import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.util.ThreadContext;

import javax.ws.rs.ext.Provider;

import static java.util.Objects.requireNonNull;

/**
 * Provider to inject a {@link Subject} as a parameter to a request that requires authentication or authorization.
 * The subject can then be used to perform finer resource restrictions then can be performed through annotations alone.
 */
@Provider
public class AuthenticatedSubjectProvider implements InjectableProvider<Authenticated, Parameter> {

    private final SecurityManager _securityManager;

    public AuthenticatedSubjectProvider(SecurityManager securityManager) {
        _securityManager = requireNonNull(securityManager, "securityManager");
    }

    @Override
    public ComponentScope getScope() {
        return ComponentScope.PerRequest;
    }

    @Override
    public Injectable getInjectable(ComponentContext componentContext, Authenticated authenticated, Parameter parameter) {
        return new Injectable() {
            @Override
            public Object getValue() {
                return new Subject(_securityManager, ThreadContext.getSubject().getPrincipals());
            }
        };
    }
}

