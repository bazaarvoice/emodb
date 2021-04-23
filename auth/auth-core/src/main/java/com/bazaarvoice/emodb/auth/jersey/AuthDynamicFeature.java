package com.bazaarvoice.emodb.auth.jersey;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.apache.shiro.mgt.SecurityManager;

import javax.ws.rs.Path;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.FeatureContext;
import javax.ws.rs.core.MultivaluedMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * {@link DynamicFeature} that places filters on the request for authentication and authorization based
 * on the presence of {@link RequiresAuthentication} and {@link RequiresPermissions} annotations on the
 * resource class and/or methods.
 *
 * Each annotation can substitute values from the path or query params into the permissions, as demonstrated in the
 * following examples:
 *
 * <code>
 * \@Path ("resource/{id}")
 * \@RequiresPermissions("resource|get|{id}")
 * \@GET
 * public Response get(@PathParam("id") String id) {
 *     ...
 * }
 *
 * \@Path ("resource/transfer")
 * \@RequiresPermissions({"resource|update|{?from}", "resource|update|{?to}"})
 * \@POST
 * public Response transfer(@QueryParam("from") String from, @QueryParam("to") to) {
 *     ...
 * }
 * </code>
 *
 *
 */
public class AuthDynamicFeature implements DynamicFeature {

    private final static Pattern SUBSTITUTION_MATCHER = Pattern.compile("\\{(?<param>(\\?[^}]|[^?}])[^}]*)}");

    private final SecurityManager _securityManager;
    private final AuthenticationTokenGenerator<?> _tokenGenerator;

    public AuthDynamicFeature(SecurityManager securityManager, AuthenticationTokenGenerator<?> tokenGenerator) {
        _securityManager = requireNonNull(securityManager, "securityManager");
        _tokenGenerator = requireNonNull(tokenGenerator, "tokenGenerator");
    }

    @Override
    public void configure(ResourceInfo resourceInfo, FeatureContext context) {
        Set<PrioritizedContainerRequestFilter> filters = Sets.newLinkedHashSet();

        // Check the resource
        RequiresPermissions permAnnotation = resourceInfo.getResourceClass().getAnnotation(RequiresPermissions.class);
        if (permAnnotation != null) {
            filters.add(new PrioritizedContainerRequestFilter(
                    new AuthorizationResourceFilter(ImmutableList.copyOf(permAnnotation.value()),
                            permAnnotation.logical(), createSubstitutionMap(permAnnotation, resourceInfo)),
                    Priorities.AUTHORIZATION));
        }

        // Check the method
        permAnnotation = resourceInfo.getResourceMethod().getAnnotation(RequiresPermissions.class);
        if (permAnnotation != null) {
            filters.add(new PrioritizedContainerRequestFilter(
                    new AuthorizationResourceFilter(ImmutableList.copyOf(permAnnotation.value()),
                            permAnnotation.logical(), createSubstitutionMap(permAnnotation, resourceInfo)),
                    Priorities.AUTHORIZATION));
        }

        // If we're doing authorization or if authentication is explicitly requested then add it as a higher priority filter
        if (!filters.isEmpty() ||
                resourceInfo.getResourceClass().getAnnotation(RequiresAuthentication.class) != null ||
                resourceInfo.getResourceMethod().getAnnotation(RequiresAuthentication.class) != null) {
            filters.add(new PrioritizedContainerRequestFilter(
                    new AuthenticationResourceFilter(_securityManager, _tokenGenerator),
                    Priorities.AUTHENTICATION));
        }

        filters.forEach(filter -> context.register(filter.getFilter(), filter.getPriority()));
    }

    private static Map<String, Function<ContainerRequestContext, String>> createSubstitutionMap(RequiresPermissions permAnnotation, ResourceInfo resourceInfo) {
        return createSubstitutionMap(permAnnotation.value(), resourceInfo);
    }

    /**
     * Returns a mapping from permissions found in the annotations to functions which can perform any necessary
     * substitutions based on actual values in the request.
     */
    private static Map<String,Function<ContainerRequestContext, String>> createSubstitutionMap(String[] permissions, ResourceInfo resourceInfo) {
        Map<String, Function<ContainerRequestContext, String>> map = Maps.newLinkedHashMap();

        for (String permission : permissions) {
            Matcher matcher = SUBSTITUTION_MATCHER.matcher(permission);
            while (matcher.find()) {
                String match = matcher.group();
                if (map.containsKey(match)) {
                    continue;
                }

                String param = matcher.group("param");
                Function<ContainerRequestContext, String> substitution;

                if (param.startsWith("?")) {
                    substitution = createQuerySubstitution(param.substring(1));
                } else {
                    substitution = createPathSubstitution(param, resourceInfo);
                }

                map.put(match, substitution);
            }
        }

        return map;
    }

    /**
     * Creates a substitution function for path values, such as
     * <code>@RequiresPermission("resource|update|{id}")</code>
     */
    private static Function<ContainerRequestContext, String> createPathSubstitution(final String param, final ResourceInfo resourceInfo) {
        int from = 0;
        int segment = -1;

        // Get the path from resource then from the method
        Path[] annotations = new Path[] { resourceInfo.getResourceClass().getAnnotation(Path.class), resourceInfo.getResourceMethod().getAnnotation(Path.class) };

        for (Path annotation : annotations) {
            if (annotation == null)  {
                continue;
            }

            int index = getSubstitutionIndex(param, annotation.value());
            if (index >= 0) {
                segment = from + index;
            } else {
                from += -index;
            }
        }

        if (segment == -1) {
            throw new IllegalArgumentException("Param not found in path: " + param);
        }

        final int validatedSegment = segment;

        return request -> request.getUriInfo().getPathSegments().get(validatedSegment).getPath();
    }

    /**
     * Gets the index in a path where the substitution parameter was found, or the negative of the number of segments
     * in the path if it was not found.  For example:
     *
     * assert(getSubstitutionIndex("id", "resource/{id}/move") == 1)
     * assert(getSubstitutionIndex("not_found", "path/with/four/segments") == -4)
     */
    private static int getSubstitutionIndex(String param, String path) {
        final String match = String.format("{%s}", param);

        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length()-1);
        }

        String[] segments = path.split("/");
        for (int i=0; i < segments.length; i++) {
            if (match.equals(segments[i])) {
                return i;
            }
        }

        return -segments.length;
    }

    /**
     * Creates a substitution function for query param values, such as
     * <code>@RequiresPermission("resource|update|{?id}")</code>
     */
    private static Function<ContainerRequestContext, String> createQuerySubstitution(final String param) {
        return new Function<ContainerRequestContext, String>() {
            @Override
            public String apply(ContainerRequestContext request) {
                MultivaluedMap<String, String> params = request.getUriInfo().getQueryParameters();
                if (!params.containsKey(param)) {
                    throw new IllegalStateException("Parameter required for authentication is missing: " + param);
                }
                List<String> values = params.get(param);
                if (values.size() != 1) {
                    throw new IllegalStateException("Exactly one parameter expected for authentication: " + param);
                }
                return values.get(0);
            }
        };
    }

    private static class PrioritizedContainerRequestFilter {
        private final ContainerRequestFilter _filter;
        private final int _priority;

        public PrioritizedContainerRequestFilter(ContainerRequestFilter filter, int priority) {
            _filter = requireNonNull(filter);
            _priority = priority;
        }

        public ContainerRequestFilter getFilter() {
            return _filter;
        }

        public int getPriority() {
            return _priority;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrioritizedContainerRequestFilter that = (PrioritizedContainerRequestFilter) o;
            return _priority == that._priority && _filter.equals(that._filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(_filter, _priority);
        }
    }
}
