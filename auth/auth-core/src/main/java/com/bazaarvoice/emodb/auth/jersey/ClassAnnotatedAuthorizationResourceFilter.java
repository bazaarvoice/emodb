package com.bazaarvoice.emodb.auth.jersey;

import org.apache.shiro.authz.annotation.Logical;

import javax.ws.rs.container.ContainerRequestContext;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


/**
 * Resource filter for Class level annotated methods which require authorization.  The subject should already be authenticated prior
 * to this filter executing.
 */
public class ClassAnnotatedAuthorizationResourceFilter extends AuthorizationResourceFilter {

    public ClassAnnotatedAuthorizationResourceFilter(List<String> permissions, Logical logical, Map<String, Function<ContainerRequestContext, String>> substitutions) {
        super(permissions, logical, substitutions);
    }
}