package com.bazaarvoice.emodb.common.dropwizard.discovery;

import com.google.common.net.HostAndPort;

import javax.ws.rs.Path;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Builder for constructing instances of {@link Payload}.
 */
public class PayloadBuilder implements Cloneable {
    public static final String DEFAULT_SCHEME = "http";
    public static final int DEFAULT_PORT = 8080;
    public static final int DEFAULT_ADMIN_PORT = 8081;
    public static final String DEFAULT_ADMIN_PATH = "";

    private String _scheme = DEFAULT_SCHEME;
    private String _host;
    private int _port = DEFAULT_PORT;
    private String _path;  // No default
    private URI _url;
    private int _adminPort = DEFAULT_ADMIN_PORT;
    private String _adminPath = DEFAULT_ADMIN_PATH;
    private URI _adminUrl;
    private Map<String, ?> _extensions = Collections.emptyMap();

    /**
     * Sets the service path from the Jersey {@link Path} annotation.
     */
    public PayloadBuilder withResource(Object resource) {
        return withPath(getPath(resource.getClass()));
    }

    /**
     * Sets the protocol, ie. "http" or "https".
     */
    public PayloadBuilder withScheme(String scheme) {
        _scheme = requireNonNull(scheme, "scheme");
        return this;
    }

    public PayloadBuilder withHostAndPort(HostAndPort host) {
        withHost(host.getHostText());
        if (host.hasPort()) {
            withPort(host.getPort());
            withAdminPort(host.getPort() + 1);
        }
        return this;
    }

    public PayloadBuilder withHost(String host) {
        _host = requireNonNull(host, "host");
        return this;
    }

    public PayloadBuilder withPort(int port) {
        checkArgument(port > 0 && port <= 65535, "Port must be >0 and <=65535");
        _port = port;
        return this;
    }

    public PayloadBuilder withPath(String path) {
        _path = requireNonNull(path, "path");
        return this;
    }

    public PayloadBuilder withUrl(URI url) {
        _url = requireNonNull(url, "url");
        return this;
    }

    public PayloadBuilder withAdminPort(int adminPort) {
        checkArgument(adminPort > 0 && adminPort <= 65535, "Admin port must be >0 and <=65535");
        _adminPort = adminPort;
        return this;
    }

    public PayloadBuilder withAdminPath(String adminPath) {
        _adminPath = requireNonNull(adminPath, "adminPath");
        return this;
    }

    public PayloadBuilder withAdminUrl(URI adminUrl) {
        _adminUrl = requireNonNull(adminUrl, "adminUrl");
        return this;
    }

    public PayloadBuilder withExtensions(Map<String, ?> extensions) {
        _extensions = extensions;
        return this;
    }

    public Payload build() {
        URI serviceUrl = constructUrl(_url, _scheme, _host, _port, _path);
        URI adminUrl = constructUrl(_adminUrl, _scheme, _host, _adminPort, _adminPath);
        return new Payload(serviceUrl, adminUrl, _extensions);
    }

    private URI constructUrl(URI overrideUrl, String scheme, String host, int port, String path) {
        if (overrideUrl != null) {
            return overrideUrl;
        }
        checkState(scheme != null, "scheme");
        checkState(host != null, "host");
        checkState(port != 0, "port");
        checkState(path != null, "path");
        return UriBuilder.fromPath(path).scheme(scheme).host(host).port(port).build();
    }

    private String getPath(Class<?> resourceType) {
        Path path = resourceType.getAnnotation(Path.class);
        requireNonNull(path, String.format("The resource %s is not annotated with @Path", resourceType.getName()));
        return path.value();
    }

    @Override
    public String toString() {
        return build().toString();
    }

    @SuppressWarnings({"CloneDoesntDeclareCloneNotSupportedException"})
    @Override
    public PayloadBuilder clone() {
        try {
            return (PayloadBuilder) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }
}
