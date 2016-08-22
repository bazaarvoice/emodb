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

import static com.google.common.base.Preconditions.checkNotNull;

public class InvalidationServiceEndPointAdapter {
    private final String _serviceName;
    private final HostAndPort _self;
    private final HostAndPort _selfAdmin;

    @Inject
    public InvalidationServiceEndPointAdapter(@InvalidationService String serviceName,
                                              @SelfHostAndPort HostAndPort self,
                                              @SelfAdminHostAndPort HostAndPort selfAdmin) {
        _serviceName = checkNotNull(serviceName, "serviceName");
        _self = checkNotNull(self, "self");
        _selfAdmin = checkNotNull(selfAdmin, "selfAdmin");
    }

    public String getServiceName() {
        return _serviceName;
    }

    public ServiceEndPoint toSelfEndPoint() {
        URI invalidateUri = UriBuilder
                .fromPath(getInvalidationTaskPath())
                .scheme("http")
                .host(_selfAdmin.getHostText())
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
        return (String) checkNotNull(payloadMap.get("invalidateUrl"), "invalidateUrl");
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
