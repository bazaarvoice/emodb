package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.common.dropwizard.guice.SelfAdminHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class InvalidationServiceEndPointAdapter {
    private final String _serviceName;
    private final HostAndPort _self;
    private final HostAndPort _selfAdmin;

    @Inject
    public InvalidationServiceEndPointAdapter(@InvalidationService String serviceName,
                                              @SelfHostAndPort HostAndPort self,
                                              @SelfAdminHostAndPort HostAndPort selfAdmin) {
        _serviceName = requireNonNull(serviceName, "serviceName");
        _self = requireNonNull(self, "self");
        _selfAdmin = requireNonNull(selfAdmin, "selfAdmin");
    }

    public String getServiceName() {
        return _serviceName;
    }

    public ServiceEndPoint toSelfEndPoint() {
        URI invalidateUri = UriBuilder
                .fromPath(getInvalidationTaskPath())
                .scheme("http")
                .host(_selfAdmin.getHost())
                .port(_selfAdmin.getPort())
                .build();
        Map<String, String> payload = ImmutableMap.of("invalidateUrl", invalidateUri.toString());
        return new ServiceEndPointBuilder()
                .withServiceName(_serviceName)
                .withId(_self.toString())
                .withPayload(JsonHelper.asJson(payload))
                .build();
    }

    public String toEndPointAddress(ServiceEndPoint endPoint) {
        String payload = endPoint.getPayload();
        Map<?,?> payloadMap = JsonHelper.fromJson(payload, Map.class);
        return (String) requireNonNull(payloadMap.get("invalidateUrl"), "invalidateUrl");
    }

    public String toEndPointAddress(URI adminUrl) {
        return UriBuilder
                .fromUri(adminUrl)
                .path(getInvalidationTaskPath())
                .build()
                .toString();
    }

    private String getInvalidationTaskPath() {
        return "/tasks/" + DropwizardInvalidationTask.NAME;
    }
}
