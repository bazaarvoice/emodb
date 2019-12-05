package com.bazaarvoice.emodb.common.dropwizard.discovery;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;

import java.net.URI;
import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * SOA (Ostrich) payload object, typically embedded within a {@link ServiceEndPoint}.
 * <p>
 * Dropwizard web servers expose a service URL (typically port 8080) which is the main RESTful
 * end point plus they expose an administration URL (typically port 8081) which is used for
 * health checks by the SOA load balancing algorithms.
 */
public class Payload {
    private final URI _serviceUrl;
    private final URI _adminUrl;
    private final Map<String, ?> _extensions;

    public static Payload valueOf(String string) {
        Map<?, ?> map = JsonHelper.fromJson(string, Map.class);
        URI serviceUri = URI.create((String) checkNotNull(map.get("serviceUrl"), "serviceUrl"));
        URI adminUri = URI.create((String) checkNotNull(map.get("adminUrl"), "adminUrl"));
        @SuppressWarnings("unchecked") Map<String, ?> extensions = MoreObjects.firstNonNull(
                (Map<String, ?>) map.get("extensions"), Collections.<String, Object>emptyMap());
        return new Payload(serviceUri, adminUri, extensions);
    }

    public Payload(URI serviceUrl, URI adminUrl) {
        this(serviceUrl, adminUrl, Collections.<String, Object>emptyMap());
    }

    public Payload(URI serviceUrl, URI adminUrl, Map<String, ?> extensions) {
        _serviceUrl = checkNotNull(serviceUrl, "serviceUrl");
        _adminUrl = checkNotNull(adminUrl, "adminUrl");
        _extensions = checkNotNull(extensions, "extensions");
    }

    public URI getServiceUrl() {
        return _serviceUrl;
    }

    public URI getAdminUrl() {
        return _adminUrl;
    }

    public Map<String, ?> getExtensions() {
        return _extensions;
    }

    @Override
    public String toString() {
        Map<String, Object> map = Maps.newLinkedHashMap();
        map.put("serviceUrl", _serviceUrl);
        map.put("adminUrl", _adminUrl);
        if (!_extensions.isEmpty()) {
            map.put("extensions", _extensions);
        }
        return JsonHelper.asJson(map);
    }
}
