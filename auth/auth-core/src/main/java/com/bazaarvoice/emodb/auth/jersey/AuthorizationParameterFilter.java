package com.bazaarvoice.emodb.auth.jersey;

import com.bazaarvoice.emodb.auth.permissions.MatchingPermission;
import com.google.common.base.Function;
import com.sun.jersey.api.core.HttpRequestContext;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import com.sun.jersey.spi.container.ResourceFilter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.annotation.Logical;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resource filter for parameters which require authorization.  The subject should already be authenticated prior
 * to this filter executing.
 */
@SuppressWarnings("Duplicates")
public class AuthorizationParameterFilter implements ResourceFilter, ContainerRequestFilter {

    private final String parameter;
    private final String[] _permissions;
    private final Logical _logical;
    private final Map<String, Function<HttpRequestContext, String>> _substitutions;

    public AuthorizationParameterFilter(String parameter,
                                        List<String> permissions,
                                        Logical logical,
                                        Map<String, Function<HttpRequestContext, String>> substitutions) {
        this.parameter = parameter;
        _permissions = permissions.toArray(new String[permissions.size()]);
        _logical = logical;
        _substitutions = substitutions;
    }

    @Override
    public ContainerRequestFilter getRequestFilter() {
        return this;
    }

    @Override
    public ContainerResponseFilter getResponseFilter() {
        return null;
    }

    /**
     * Authorizes the client for the annotated permissions.  If any authorizations fail an {@link AuthorizationException}
     * will be thrown, otherwise the original request is returned.
     */
    @Override
    public ContainerRequest filter(ContainerRequest request) {
        if (request.getQueryParameters().containsKey(parameter)) {
            Subject subject = ThreadContext.getSubject();

            String[] permissions = resolvePermissions(request);

            if (permissions.length == 1 || _logical == Logical.AND) {
                // Shortcut call to check all permissions at once
                subject.checkPermissions(permissions);
            } else {
                // Check each permission until any passes
                boolean anyPermitted = false;
                int p = 0;
                while (!anyPermitted) {
                    try {
                        subject.checkPermission(permissions[p]);
                        anyPermitted = true;
                    } catch (AuthorizationException e) {
                        // If this is the last permission then pass the exception along
                        if (++p == permissions.length) {
                            throw e;
                        }
                    }
                }
            }
        }

        return request;
    }

    /**
     * Resolves permissions based on the request.  For example, if the annotation's permission is
     * "get|{thing}" and the method's @Path annotation is "/resources/{thing}" then a request to
     * "/resources/table" will resolve to the permission "get|table".
     */
    private String[] resolvePermissions(ContainerRequest request) {
        String[] values = _permissions;

        if (_substitutions.isEmpty()) {
            return values;
        }

        String[] permissions = new String[values.length];
        System.arraycopy(values, 0, permissions, 0, values.length);

        for (Map.Entry<String, Function<HttpRequestContext, String>> entry : _substitutions.entrySet()) {
            String key = Pattern.quote(entry.getKey());
            String substitution = Matcher.quoteReplacement(MatchingPermission.escape(entry.getValue().apply(request)));

            for (int i=0; i < values.length; i++) {
                permissions[i] = permissions[i].replaceAll(key, substitution);
            }
        }

        return permissions;
    }
}
