package com.bazaarvoice.emodb.auth.jersey;

import javax.inject.Inject;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.util.ThreadContext;
import org.glassfish.jersey.server.internal.inject.AbstractContainerRequestValueFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class AuthenticatedSubjectFactory extends AbstractContainerRequestValueFactory<Subject> {

    private final SecurityManager _securityManager;

    @Inject
    public AuthenticatedSubjectFactory(SecurityManager securityManager) {
        _securityManager = checkNotNull(securityManager);
    }

    @Override
    public Subject provide() {
        return new Subject(_securityManager, ThreadContext.getSubject().getPrincipals());
    }
}