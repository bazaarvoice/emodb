package com.bazaarvoice.emodb.auth.jersey;

import javax.inject.Singleton;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;
import org.apache.shiro.mgt.SecurityManager;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.InjectionResolver;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.spi.internal.ValueFactoryProvider;

import static java.util.Objects.requireNonNull;

public class AuthenticatedSubjectFeature implements Feature {

    private final SecurityManager _securityManager;

    public AuthenticatedSubjectFeature(SecurityManager securityManager) {
        _securityManager = requireNonNull(securityManager);
    }

    @Override
    public boolean configure(FeatureContext context) {
        context.register(new AbstractBinder() {
            @Override
            protected void configure() {
                bindFactory(new SecurityManagerFactory()).to(SecurityManager.class);
                bind(AuthenticatedSubjectFactory.class)
                        .to(AuthenticatedSubjectFactory.class)
                        .in(Singleton.class);
                bind(AuthenticatedSubjectProvider.class)
                        .to(ValueFactoryProvider.class)
                        .in(Singleton.class);
                bind(AuthenticatedSubjectParamInjectionResolver.class)
                        .to(new TypeLiteral<InjectionResolver<Authenticated>>(){})
                        .in(Singleton.class);
            }
        });
        return true;
    }

    private class SecurityManagerFactory implements Factory<SecurityManager> {

        @Override
        public SecurityManager provide() {
            return _securityManager;
        }

        @Override
        public void dispose(SecurityManager s) {
        }
    }
}
