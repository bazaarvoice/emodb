package com.bazaarvoice.emodb.client2.uri;

import javax.ws.rs.Path;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriBuilderException;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

/**
 * A modified version of Jersey's UriBuilder which does not contextually encode any path
 * elements and has no Jersey dependencies.  For example, in the UriBuilderImpl the following is true:
 *
 * <code>
 *     UriBuilderImpl builder = ...;
 *     assertTrue(builder.segment("above%2fbelow").build().getRawPath().endsWith("/above%2fbelow"));
 * </code>
 *
 * With this implementation the following is true:
 *
 * <code>
 *     EmoUriBuilder builder = ...;
 *     assertTrue(builder.segment("above%2fbelow").build().getRawPath().endsWith("/above%252fbelow"));
 * </code>
 *
 * Modifications to the original code are commented with EMODB-MODIFICATION.
 */
public class EmoUriBuilder2 extends UriBuilder {

    // All fields should be in the percent-encoded form
    private String scheme;
    private String ssp;
    private String authority;
    private String userInfo;
    private String host;
    private int port = -1;
    private final StringBuilder path;
    private MultivaluedMap<String, String> matrixParams;
    private final StringBuilder query;
    private MultivaluedMap<String, String> queryParams;
    private String fragment;

    public EmoUriBuilder2() {
        path = new StringBuilder();
        query = new StringBuilder();
    }

    private EmoUriBuilder2(EmoUriBuilder2 that) {
        this.scheme = that.scheme;
        this.ssp = that.ssp;
        this.authority = that.authority;
        this.userInfo = that.userInfo;
        this.host = that.host;
        this.port = that.port;
        this.path = new StringBuilder(that.path);
        // EMODB-MODIFICATION Replaced with EmoMultivaluedMap implementation
        this.matrixParams = that.matrixParams == null ? null : EmoMultivaluedMap2.copy(that.matrixParams);
        this.query = new StringBuilder(that.query);
        // EMODB-MODIFICATION Replaced with EmoMultivaluedMap implementation
        this.queryParams = that.queryParams == null ? null : EmoMultivaluedMap2.copy(that.queryParams);
        this.fragment = that.fragment;
    }

    @Override
    public EmoUriBuilder2 clone() {
        return new EmoUriBuilder2(this);
    }

    /**
     * EMODB-MODIFICATION
     * This method has been added as a convenience to mirror the more typical constructor using
     * {@link UriBuilder#fromUri(URI)}.
     */
    public static UriBuilder fromUri(URI uri) {
        return new EmoUriBuilder2().uri(uri);
    }

    /**
     * EMODB-MODIFICATION
     * This method has been added as a convenience to mirror the more typical constructor using
     * {@link UriBuilder#fromUri(String)}.
     */
    public static UriBuilder fromUri(String uri) {
        return new EmoUriBuilder2().uri(URI.create(uri));
    }

    @Override
    public UriBuilder uri(URI uri) {
        if (uri == null) {
            throw new IllegalArgumentException("URI parameter is null");
        }

        if (uri.getRawFragment() != null) {
            fragment = uri.getRawFragment();
        }

        if (uri.isOpaque()) {
            scheme = uri.getScheme();
            ssp = uri.getRawSchemeSpecificPart();
            return this;
        }

        if (uri.getScheme() == null) {
            if (ssp != null) {
                if (uri.getRawSchemeSpecificPart() != null) {
                    ssp = uri.getRawSchemeSpecificPart();
                    return this;
                }
            }
        } else {
            scheme = uri.getScheme();
        }

        ssp = null;
        if (uri.getRawAuthority() != null) {
            if (uri.getRawUserInfo() == null && uri.getHost() == null && uri.getPort() == -1) {
                authority = uri.getRawAuthority();
                userInfo = null;
                host = null;
                port = -1;
            } else {
                authority = null;
                if (uri.getRawUserInfo() != null) {
                    userInfo = uri.getRawUserInfo();
                }
                if (uri.getHost() != null) {
                    host = uri.getHost();
                }
                if (uri.getPort() != -1) {
                    port = uri.getPort();
                }
            }
        }

        if (uri.getRawPath() != null && uri.getRawPath().length() > 0) {
            path.setLength(0);
            path.append(uri.getRawPath());
        }
        if (uri.getRawQuery() != null && uri.getRawQuery().length() > 0) {
            query.setLength(0);
            query.append(uri.getRawQuery());

        }

        return this;
    }

    @Override
    public UriBuilder uri(String uri) {
        return new EmoUriBuilder2().uri(URI.create(uri));
    }

    @Override
    public UriBuilder scheme(String scheme) {
        if (scheme != null) {
            this.scheme = scheme;
            EmoUriComponent2.validate(scheme, EmoUriComponent2.Type.SCHEME, true);
        } else {
            this.scheme = null;
        }
        return this;
    }

    @Override
    public UriBuilder schemeSpecificPart(String ssp) {
        if (ssp == null) {
            throw new IllegalArgumentException("Scheme specific part parameter is null");
        }

        // TODO encode or validate scheme specific part
        // This will not work for template variables present in the spp
        StringBuilder sb = new StringBuilder();
        if (scheme != null) {
            sb.append(scheme).append(':');
        }
        if (ssp != null) {
            sb.append(ssp);
        }
        if (fragment != null && fragment.length() > 0) {
            sb.append('#').append(fragment);
        }
        URI uri = createURI(sb.toString());

        if (uri.getRawSchemeSpecificPart() != null && uri.getRawPath() == null) {
            this.ssp = uri.getRawSchemeSpecificPart();
        } else {
            this.ssp = null;

            if (uri.getRawAuthority() != null) {
                if (uri.getRawUserInfo() == null && uri.getHost() == null && uri.getPort() == -1) {
                    authority = uri.getRawAuthority();
                    userInfo = null;
                    host = null;
                    port = -1;
                } else {
                    authority = null;
                    userInfo = uri.getRawUserInfo();
                    host = uri.getHost();
                    port = uri.getPort();
                }
            }

            path.setLength(0);
            path.append(replaceNull(uri.getRawPath()));

            query.setLength(0);
            query.append(replaceNull(uri.getRawQuery()));
        }
        return this;
    }

    @Override
    public UriBuilder userInfo(String ui) {
        checkSsp();
        this.userInfo = (ui != null)
                ? encode(ui, EmoUriComponent2.Type.USER_INFO) : null;
        return this;
    }

    @Override
    public UriBuilder host(String host) {
        checkSsp();
        if (host != null) {
            if (host.length() == 0) // null is used to reset host setting
            {
                throw new IllegalArgumentException("Invalid host name");
            }
            this.host = encode(host, EmoUriComponent2.Type.HOST);
        } else {
            this.host = null;
        }
        return this;
    }

    @Override
    public UriBuilder port(int port) {
        checkSsp();
        if (port < -1) // -1 is used to reset port setting and since URI allows
        // as port any positive integer, so do we.
        {
            throw new IllegalArgumentException("Invalid port value");
        }
        this.port = port;
        return this;
    }

    @Override
    public UriBuilder replacePath(String path) {
        checkSsp();
        this.path.setLength(0);
        if (path != null) {
            appendPath(path);
        }
        return this;
    }

    @Override
    public UriBuilder path(String path) {
        checkSsp();
        appendPath(path);
        return this;
    }

    @Override
    public UriBuilder path(Class resource) {
        checkSsp();
        if (resource == null) {
            throw new IllegalArgumentException("Resource parameter is null");
        }

        Class<?> c = resource;
        Path p = c.getAnnotation(Path.class);
        if (p == null) {
            throw new IllegalArgumentException("The class, " + resource + " is not annotated with @Path");
        }
        appendPath(p);
        return this;
    }

    @Override
    public UriBuilder path(Class resource, String methodName) {
        checkSsp();
        if (resource == null) {
            throw new IllegalArgumentException("Resource parameter is null");
        }
        if (methodName == null) {
            throw new IllegalArgumentException("MethodName parameter is null");
        }

        Method[] methods = resource.getMethods();
        Method found = null;
        for (Method m : methods) {
            if (methodName.equals(m.getName())) {
                if (found == null) {
                    found = m;
                } else {
                    throw new IllegalArgumentException();
                }
            }
        }

        if (found == null) {
            throw new IllegalArgumentException("The method named, " + methodName
                    + ", is not specified by " + resource);
        }

        appendPath(getPath(found));

        return this;
    }

    @Override
    public UriBuilder path(Method method) {
        checkSsp();
        if (method == null) {
            throw new IllegalArgumentException("Method is null");
        }
        appendPath(getPath(method));
        return this;
    }

    private Path getPath(AnnotatedElement ae) {
        Path p = ae.getAnnotation(Path.class);
        if (p == null) {
            throw new IllegalArgumentException("The annotated element, "
                    + ae + " is not annotated with @Path");
        }
        return p;
    }

    @Override
    public UriBuilder segment(String... segments) throws IllegalArgumentException {
        checkSsp();
        if (segments == null) {
            throw new IllegalArgumentException("Segments parameter is null");
        }

        for (String segment : segments) {
            appendPath(segment, true);
        }
        return this;
    }

    @Override
    public UriBuilder replaceMatrix(String matrix) {
        checkSsp();
        int i = path.lastIndexOf("/");
        if (i != -1) {
            i = 0;
        }
        i = path.indexOf(";", i);
        if (i != -1) {
            path.setLength(i + 1);
        } else {
            path.append(';');
        }

        if (matrix != null) {
            path.append(encode(matrix, EmoUriComponent2.Type.PATH));
        }
        return this;
    }

    @Override
    public UriBuilder matrixParam(String name, Object... values) {
        checkSsp();
        if (name == null) {
            throw new IllegalArgumentException("Name parameter is null");
        }
        if (values == null) {
            throw new IllegalArgumentException("Value parameter is null");
        }
        if (values.length == 0) {
            return this;
        }

        name = encode(name, EmoUriComponent2.Type.MATRIX_PARAM);
        if (matrixParams == null) {
            for (Object value : values) {
                path.append(';').append(name);

                if (value == null) {
                    throw new IllegalArgumentException("One or more of matrix value parameters are null");
                }

                final String stringValue = value.toString();
                if (stringValue.length() > 0) {
                    path.append('=').append(encode(stringValue, EmoUriComponent2.Type.MATRIX_PARAM));
                }
            }
        } else {
            for (Object value : values) {
                if (value == null) {
                    throw new IllegalArgumentException("One or more of matrix value parameters are null");
                }

                matrixParams.add(name, encode(value.toString(), EmoUriComponent2.Type.MATRIX_PARAM));
            }
        }
        return this;
    }

    @Override
    public UriBuilder replaceMatrixParam(String name, Object... values) {
        checkSsp();

        if (name == null) {
            throw new IllegalArgumentException("Name parameter is null");
        }

        if (matrixParams == null) {
            int i = path.lastIndexOf("/");
            if (i != -1) {
                i = 0;
            }
            matrixParams = EmoUriComponent2.decodeMatrix((i != -1) ? path.substring(i) : "", false);
            i = path.indexOf(";", i);
            if (i != -1) {
                path.setLength(i);
            }
        }

        name = encode(name, EmoUriComponent2.Type.MATRIX_PARAM);
        matrixParams.remove(name);
        if (values != null) {
            for (Object value : values) {
                if (value == null) {
                    throw new IllegalArgumentException("One or more of matrix value parameters are null");
                }

                matrixParams.add(name, encode(value.toString(), EmoUriComponent2.Type.MATRIX_PARAM));
            }
        }
        return this;
    }

    @Override
    public UriBuilder replaceQuery(String query) {
        checkSsp();
        this.query.setLength(0);
        if (query != null) {
            this.query.append(encode(query, EmoUriComponent2.Type.QUERY));
        }
        return this;
    }

    @Override
    public UriBuilder queryParam(String name, Object... values) {
        checkSsp();
        if (name == null) {
            throw new IllegalArgumentException("Name parameter is null");
        }
        if (values == null) {
            throw new IllegalArgumentException("Value parameter is null");
        }
        if (values.length == 0) {
            return this;
        }

        name = encode(name, EmoUriComponent2.Type.QUERY_PARAM);
        if (queryParams == null) {
            for (Object value : values) {
                if (query.length() > 0) {
                    query.append('&');
                }
                query.append(name);

                if (value == null) {
                    throw new IllegalArgumentException("One or more of query value parameters are null");
                }

                query.append('=').append(encode(value.toString(), EmoUriComponent2.Type.QUERY_PARAM));
            }
        } else {
            for (Object value : values) {
                if (value == null) {
                    throw new IllegalArgumentException("One or more of query value parameters are null");
                }

                queryParams.add(name, encode(value.toString(), EmoUriComponent2.Type.QUERY_PARAM));
            }
        }
        return this;
    }

    @Override
    public UriBuilder replaceQueryParam(String name, Object... values) {
        checkSsp();

        if (queryParams == null) {
            queryParams = EmoUriComponent2.decodeQuery(query.toString(), false);
            query.setLength(0);
        }

        name = encode(name, EmoUriComponent2.Type.QUERY_PARAM);
        queryParams.remove(name);

        if (values == null) {
            return this;
        }

        for (Object value : values) {
            if (value == null) {
                throw new IllegalArgumentException("One or more of query value parameters are null");
            }

            queryParams.add(name, encode(value.toString(), EmoUriComponent2.Type.QUERY_PARAM));
        }
        return this;
    }

    @Override
    public UriBuilder fragment(String fragment) {
        this.fragment = (fragment != null)
                ? encode(fragment, EmoUriComponent2.Type.FRAGMENT)
                : null;
        return this;
    }

    @Override
    public UriBuilder resolveTemplate(String s, Object o) {
        return null;
    }

    @Override
    public UriBuilder resolveTemplate(String s, Object o, boolean b) {
        return null;
    }

    @Override
    public UriBuilder resolveTemplateFromEncoded(String s, Object o) {
        return null;
    }

    @Override
    public UriBuilder resolveTemplates(Map<String, Object> map) {
        return null;
    }

    @Override
    public UriBuilder resolveTemplates(Map<String, Object> map, boolean b) throws IllegalArgumentException {
        return null;
    }

    @Override
    public UriBuilder resolveTemplatesFromEncoded(Map<String, Object> map) {
        return null;
    }

    private void checkSsp() {
        if (ssp != null) {
            throw new IllegalArgumentException("Schema specific part is opaque");
        }
    }

    private void appendPath(Path t) {
        if (t == null) {
            throw new IllegalArgumentException("Path is null");
        }

        appendPath(t.value());
    }

    private void appendPath(String path) {
        appendPath(path, false);
    }

    private void appendPath(String segments, boolean isSegment) {
        if (segments == null) {
            throw new IllegalArgumentException("Path segment is null");
        }
        if (segments.length() == 0) {
            return;
        }

        // Encode matrix parameters on current path segment
        encodeMatrix();

        segments = encode(segments,
                (isSegment) ? EmoUriComponent2.Type.PATH_SEGMENT : EmoUriComponent2.Type.PATH);

        final boolean pathEndsInSlash = path.length() > 0 && path.charAt(path.length() - 1) == '/';
        final boolean segmentStartsWithSlash = segments.charAt(0) == '/';

        if (path.length() > 0 && !pathEndsInSlash && !segmentStartsWithSlash) {
            path.append('/');
        } else if (pathEndsInSlash && segmentStartsWithSlash) {
            segments = segments.substring(1);
            if (segments.length() == 0) {
                return;
            }
        }

        path.append(segments);
    }

    private void encodeMatrix() {
        if (matrixParams == null || matrixParams.isEmpty()) {
            return;
        }

        for (Map.Entry<String, List<String>> e : matrixParams.entrySet()) {
            String name = e.getKey();

            for (String value : e.getValue()) {
                path.append(';').append(name);
                if (value.length() > 0) {
                    path.append('=').append(value);
                }
            }
        }
        matrixParams = null;
    }

    private void encodeQuery() {
        if (queryParams == null || queryParams.isEmpty()) {
            return;
        }

        for (Map.Entry<String, List<String>> e : queryParams.entrySet()) {
            String name = e.getKey();

            for (String value : e.getValue()) {
                if (query.length() > 0) {
                    query.append('&');
                }
                query.append(name).append('=').append(value);
            }
        }
        queryParams = null;
    }

    private String encode(String s, EmoUriComponent2.Type type) {
        // EMODB-MODIFICATION:
        // return EmoUriComponent.contextualEncode(s, type, true);
        return EmoUriComponent2.encode(s, type, false);
    }


    @Override
    public URI buildFromMap(Map<String, ?> values) {
        // EMODB-MODIFICATION:  Templates are not supported, so buildFromMap is not supported
        throw new UnsupportedOperationException("Templates not supported");
    }

    @Override
    public URI buildFromMap(Map<String, ?> map, boolean b) throws IllegalArgumentException, UriBuilderException {
        // EMODB-MODIFICATION:  Templates are not supported, so buildFromMap is not supported
        throw new UnsupportedOperationException("Templates not supported for buildFromMap(Map, boolean)");
    }

    @Override
    public URI buildFromEncodedMap(Map<String, ?> values) throws IllegalArgumentException, UriBuilderException {
        // EMODB-MODIFICATION:  Templates are not supported, so buildFromMap is not supported
        throw new UnsupportedOperationException("Templates not supported");
    }

    @Override
    public URI build(Object... values) {
        return _build(true, values);
    }

    @Override
    public URI build(Object[] objects, boolean b) throws IllegalArgumentException, UriBuilderException {
        // EMODB-MODIFICATION:  Templates are not supported, so build is not supported
        throw new UnsupportedOperationException("Templates not supported, can not build");

    }

    @Override
    public URI buildFromEncoded(Object... values) {
        return _build(false, values);
    }

    @Override
    public String toTemplate() {
        // EMODB-MODIFICATION:  Templates are not supported, so toTemplate()  is not supported
        throw new UnsupportedOperationException("Templates not supported, so can not convert to template");
    }

    private URI _build(boolean encode, Object... values) {
        if (values == null || values.length == 0) {
            return createURI(create());
        }

        // EMODB-MODIFICATION:  Templates are not supported, so the list of values must have been empty
        throw new UnsupportedOperationException("UriBuilder templates are not supported");

//        if (ssp != null) {
//            throw new IllegalArgumentException("Schema specific part is opaque");
//        }
//
//        encodeMatrix();
//        encodeQuery();
//
//        String uri = UriTemplate.createURI(
//                scheme, authority,
//                userInfo, host, (port != -1) ? String.valueOf(port) : null,
//                path.toString(), query.toString(), fragment, values, encode);
//        return createURI(uri);
    }

    private String create() {
        encodeMatrix();
        encodeQuery();

        StringBuilder sb = new StringBuilder();

        if (scheme != null) {
            sb.append(scheme).append(':');
        }

        if (ssp != null) {
            sb.append(ssp);
        } else {
            if (userInfo != null || host != null || port != -1) {
                sb.append("//");

                if (userInfo != null && userInfo.length() > 0) {
                    sb.append(userInfo).append('@');
                }

                if (host != null) {
                    // TODO check IPv6 address
                    sb.append(host);
                }

                if (port != -1) {
                    sb.append(':').append(port);
                }
            } else if (authority != null) {
                sb.append("//").append(authority);
            }

            if (path.length() > 0) {
                if (sb.length() > 0 && path.charAt(0) != '/') {
                    sb.append("/");
                }
                sb.append(path);
            }

            if (query.length() > 0) {
                sb.append('?').append(query);
            }
        }

        if (fragment != null && fragment.length() > 0) {
            sb.append('#').append(fragment);
        }

        return EmoUriComponent2.encodeTemplateNames(sb.toString());
    }

    private URI createURI(String uri) {
        try {
            return new URI(uri);
        } catch (URISyntaxException ex) {
            throw new UriBuilderException(ex);
        }
    }

    private String replaceNull(String s) {
        return (s != null) ? s : "";
    }
}
