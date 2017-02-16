package com.bazaarvoice.emodb.auth.jersey;

import com.bazaarvoice.emodb.auth.ParamRequiresPermissions;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.jersey.api.core.HttpRequestContext;
import com.sun.jersey.api.model.AbstractMethod;
import com.sun.jersey.spi.container.ResourceFilter;
import com.sun.jersey.spi.container.ResourceFilterFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.shiro.authz.annotation.Logical;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.apache.shiro.mgt.SecurityManager;

import javax.annotation.Nullable;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MultivaluedMap;
import java.lang.reflect.Parameter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link ResourceFilterFactory} that places filters on the request for authentication and authorization based
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
 * public ClientResponse get(@PathParam("id") String id) {
 *     ...
 * }
 *
 * \@Path ("resource/transfer")
 * \@RequiresPermissions({"resource|update|{?from}", "resource|update|{?to}"})
 * \@POST
 * public ClientResponse transfer(@QueryParam("from") String from, @QueryParam("to") to) {
 *     ...
 * }
 * </code>
 *
 *
 */
public class AuthResourceFilterFactory implements ResourceFilterFactory {

    private final static Pattern SUBSTITUTION_MATCHER = Pattern.compile("\\{(?<param>(\\?.|[^\\?]).*)\\}");

    private final SecurityManager _securityManager;
    private final AuthenticationTokenGenerator<?> _tokenGenerator;

    public AuthResourceFilterFactory(SecurityManager securityManager, AuthenticationTokenGenerator<?> tokenGenerator) {
        _securityManager = checkNotNull(securityManager, "securityManager");
        _tokenGenerator = checkNotNull(tokenGenerator, "tokenGenerator");
    }

    @Override
    public List<ResourceFilter> create(AbstractMethod am) {

        LinkedList<ResourceFilter> filters = Lists.newLinkedList();

        // Check the resource
        {
            final RequiresPermissions permAnnotation = am.getResource().getAnnotation(RequiresPermissions.class);
            if (permAnnotation != null) {
                filters.add(new AuthorizationResourceFilter(
                    ImmutableList.copyOf(permAnnotation.value()),
                    permAnnotation.logical(),
                    createSubstitutionMap(permAnnotation.value(), am)
                ));
            }
        }

        // Check the method
        {
            final RequiresPermissions permAnnotation = am.getAnnotation(RequiresPermissions.class);
            if (permAnnotation != null) {
                filters.add(new AuthorizationResourceFilter(
                    ImmutableList.copyOf(permAnnotation.value()),
                    permAnnotation.logical(),
                    createSubstitutionMap(permAnnotation.value(), am)
                ));
            }
        }

        // Check the parameters
        {
            for (Parameter parameter: am.getMethod().getParameters()) {
                final ParamRequiresPermissions permAnnotation = parameter.getAnnotation(ParamRequiresPermissions.class);
                if (permAnnotation != null) {
                    final QueryParam queryParamAnnotation = parameter.getAnnotation(QueryParam.class);
                    filters.add(new AuthorizationParameterFilter(
                        queryParamAnnotation.value(),
                        ImmutableList.copyOf(permAnnotation.value()),
                        adapt(permAnnotation.logical()),
                        createSubstitutionMap(permAnnotation.value(), am)
                    ));
                }
            }
        }

        // If we're doing authorization or if authentication is explicitly requested then add it as the first filter
        if (!filters.isEmpty() ||
                am.getResource().getAnnotation(RequiresAuthentication.class) != null ||
                am.getAnnotation(RequiresAuthentication.class) != null) {
            filters.addFirst(new AuthenticationResourceFilter(_securityManager, _tokenGenerator));
        }

        return filters;
    }

    private Logical adapt(final ParamRequiresPermissions.CustomLogical customLogical) {
        switch (customLogical) {
            case AND:
                return Logical.AND;
            case OR:
                return Logical.OR;
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Returns a mapping from permissions found in the annotations to functions which can perform any necessary
     * substitutions based on actual values in the request.
     */
    private Map<String,Function<HttpRequestContext, String>> createSubstitutionMap(String[] permissions, AbstractMethod am) {
        Map<String, Function<HttpRequestContext, String>> map = Maps.newLinkedHashMap();

        for (String permission : permissions) {
            Matcher matcher = SUBSTITUTION_MATCHER.matcher(permission);
            while (matcher.find()) {
                String match = matcher.group();
                if (map.containsKey(match)) {
                    continue;
                }

                String param = matcher.group("param");
                Function<HttpRequestContext, String> substitution;

                if (param.startsWith("?")) {
                    substitution = createQuerySubstitution(param.substring(1));
                } else {
                    substitution = createPathSubstitution(param, am);
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
    private Function<HttpRequestContext, String> createPathSubstitution(final String param, final AbstractMethod am) {
        int from = 0;
        int segment = -1;

        // Get the path from resource then from the method
        Path[] annotations = new Path[] { am.getResource().getAnnotation(Path.class), am.getAnnotation(Path.class) };

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

        return new Function<HttpRequestContext, String>() {
            @Override
            public String apply(HttpRequestContext request) {
                return request.getPathSegments().get(validatedSegment).getPath();
            }
        };
    }

    /**
     * Gets the index in a path where the substitution parameter was found, or the negative of the number of segments
     * in the path if it was not found.  For example:
     *
     * assert(getSubstitutionIndex("id", "resource/{id}/move") == 1)
     * assert(getSubstitutionIndex("not_found", "path/with/four/segments") == -4)
     */
    private int getSubstitutionIndex(String param, String path) {
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
    private Function<HttpRequestContext, String> createQuerySubstitution(final String param) {
        return new Function<HttpRequestContext, String>() {
            @Override
            public String apply(HttpRequestContext request) {
                MultivaluedMap<String, String> params = request.getQueryParameters();
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
}
