package com.bazaarvoice.emodb.auth.jersey;

import org.glassfish.jersey.server.internal.inject.ParamInjectionResolver;

public class AuthenticatedSubjectParamInjectionResolver extends ParamInjectionResolver {
    public AuthenticatedSubjectParamInjectionResolver() {
        super(AuthenticatedSubjectProvider.class);
    }
}

