package com.bazaarvoice.emodb.common.dropwizard.discovery;

import com.google.common.net.HostAndPort;

import java.net.URI;
import java.util.Map;

/**
 * YAML-friendly {@link Payload} builder.
 * <p>
 * In conjuction with {@link ConfiguredFixedHostDiscoverySource}, a list of Dropwizard end points can be configured
 * using a terse YAML format:
 * <p>
 * Example YAML where all settings use their default values where the host name is unique and can be used as the end
 * point id (the host property defaults to the endpoint id):
 * <pre>
 * ec2-184-72-181-173.compute-1.amazonaws.com: {}
 * ec2-75-101-213-222.compute-1.amazonaws.com: {}
 * </pre>
 * Example YAML where the host name is not unique and non-default ports are used:
 * <pre>
 * local-server-1:
 *     host:        localhost
 *     port:        8080
 *     adminPort:   8081
 * local-server-2:
 *     host:        localhost
 *     port:        8090
 *     adminPort:   8091
 * </pre>
 * On rare occasions it may be necessary to specify the complete service and admin URLs:
 * <pre>
 * localhost:
 *     serviceUrl:  http://localhost:8080/mysvc/1
 *     adminUrl:    http://localhost:8081
 * </pre>
 */
public class ConfiguredPayload {
    private final PayloadBuilder _builder = new PayloadBuilder();
    private boolean _hasHost;

    public void setScheme(String scheme) {
        _builder.withScheme(scheme);
    }

    public ConfiguredPayload withScheme(String scheme) {
        _builder.withScheme(scheme);
        return this;
    }

    public void setHost(String host) {
        _builder.withHost(host);
        _hasHost = true;
    }

    public void setPort(int port) {
        _builder.withPort(port);
    }

    public void setAdminPort(int adminPort) {
        _builder.withAdminPort(adminPort);
    }

    public void setServiceUrl(URI serviceUrl) {
        _builder.withUrl(serviceUrl);
    }

    public void setAdminUrl(URI adminUrl) {
        _builder.withAdminUrl(adminUrl);
    }

    public void setExtensions(Map<String, ?> extensions) {
        _builder.withExtensions(extensions);
    }

    public Payload toPayload(String endPointId, String servicePath) {
        PayloadBuilder builder = _builder.clone();
        if (!_hasHost) {
            builder.withHostAndPort(HostAndPort.fromString(endPointId));
        }
        builder.withPath(servicePath);
        return builder.build();
    }

    @Override
    public String toString() {
        return _builder.toString();
    }
}
