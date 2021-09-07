package com.bazaarvoice.emodb.auth.jersey;

import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.util.ThreadContext;
import org.glassfish.jersey.server.internal.inject.AbstractContainerRequestValueFactory;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class AuthenticatedSubjectFactory extends AbstractContainerRequestValueFactory<Subject> {

    private final SecurityManager _securityManager;

    @Inject
    public AuthenticatedSubjectFactory(SecurityManager securityManager) {
        _securityManager = requireNonNull(securityManager);
    }

    @Override
    public Subject provide() {
        return new Subject(_securityManager, ThreadContext.getSubject().getPrincipals());
    }
}
