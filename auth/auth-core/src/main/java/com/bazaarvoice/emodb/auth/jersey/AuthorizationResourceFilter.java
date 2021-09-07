package com.bazaarvoice.emodb.auth.jersey;

import com.bazaarvoice.emodb.auth.permissions.MatchingPermission;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.annotation.Logical;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base Resource filter class. Use the sub classes ClassAnnotatedAuthorizationResourceFilter and
 * MethodAnnotatedAuthorizationResourceFilter to prevent component register errors.
 */
public class AuthorizationResourceFilter implements ContainerRequestFilter {

    private final String[] _permissions;
    private final Logical _logical;
    private final Map<String, Function<ContainerRequestContext, String>> _substitutions;

    public AuthorizationResourceFilter(List<String> permissions, Logical logical,
                                       Map<String, Function<ContainerRequestContext, String>> substitutions) {
        _permissions = permissions.toArray(new String[permissions.size()]);
        _logical = logical;
        _substitutions = substitutions;
    }

    /**
     * Authorizes the client for the annotated permissions.  If any authorizations fail an {@link AuthorizationException}
     * will be thrown, otherwise the original request is returned.
     */
    @Override
    public void filter(ContainerRequestContext request) {
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

    /**
     * Resolves permissions based on the request.  For example, if the annotation's permission is
     * "get|{thing}" and the method's @Path annotation is "/resources/{thing}" then a request to
     * "/resources/table" will resolve to the permission "get|table".
     */
    private String[] resolvePermissions(ContainerRequestContext request) {
        String[] values = _permissions;

        if (_substitutions.isEmpty()) {
            return values;
        }

        String[] permissions = new String[values.length];
        System.arraycopy(values, 0, permissions, 0, values.length);

        for (Map.Entry<String, Function<ContainerRequestContext, String>> entry : _substitutions.entrySet()) {
            String key = Pattern.quote(entry.getKey());
            String substitution = Matcher.quoteReplacement(MatchingPermission.escape(entry.getValue().apply(request)));

            for (int i = 0; i < values.length; i++) {
                permissions[i] = permissions[i].replaceAll(key, substitution);
            }
        }

        return permissions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AuthorizationResourceFilter that = (AuthorizationResourceFilter) o;
        return Arrays.equals(_permissions, that._permissions) && _logical == that._logical && _substitutions.equals(that._substitutions);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(_logical, _substitutions);
        result = 31 * result + Arrays.hashCode(_permissions);
        return result;
    }
}
