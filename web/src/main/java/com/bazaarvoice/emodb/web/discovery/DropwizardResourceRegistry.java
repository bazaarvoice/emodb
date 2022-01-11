package com.bazaarvoice.emodb.web.discovery;

import com.bazaarvoice.emodb.common.dropwizard.discovery.ManagedRegistration;
import com.bazaarvoice.emodb.common.dropwizard.discovery.Payload;
import com.bazaarvoice.emodb.common.dropwizard.discovery.PayloadBuilder;
import com.bazaarvoice.emodb.common.dropwizard.discovery.ResourceRegistry;
import com.bazaarvoice.emodb.common.dropwizard.discovery.ServiceNames;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfAdminHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.bazaarvoice.ostrich.ServiceRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.dropwizard.setup.Environment;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Registers a Jersey resource with Dropwizard and ZooKeeper.
 */
public class DropwizardResourceRegistry implements ResourceRegistry {
    private final Environment _environment;
    private final ServiceRegistry _serviceRegistry;
    private final HostAndPort _self;
    private final HostAndPort _selfAdmin;

    @Inject
    public DropwizardResourceRegistry(Environment environment, ServiceRegistry serviceRegistry,
                                      @SelfHostAndPort HostAndPort self, @SelfAdminHostAndPort HostAndPort selfAdmin) {
        _environment = requireNonNull(environment, "environment");
        _serviceRegistry = requireNonNull(serviceRegistry, "serviceRegistry");
        _self = requireNonNull(self, "self");
        _selfAdmin = requireNonNull(selfAdmin, "selfAdmin");
        checkArgument(self.getHost().equals(selfAdmin.getHost()));
    }

    /**
     * Adds a Jersey resource annotated with the {@link javax.ws.rs.Path} annotation to the Dropwizard environment and
     * registers it with the SOA {@link com.bazaarvoice.ostrich.ServiceRegistry}.
     */
    @Override
    public void addResource(String namespace, String service, Object resource) {
        ServiceEndPoint endPoint = toEndPoint(namespace, service, resource);
        _environment.lifecycle().manage(new ManagedRegistration(_serviceRegistry, endPoint));
        _environment.jersey().register(resource);
    }

    private ServiceEndPoint toEndPoint(String namespace, String service, Object resource) {
        String serviceName = ServiceNames.forNamespaceAndBaseServiceName(namespace, service);

        Payload payload = new PayloadBuilder()
                .withResource(resource)
                .withHostAndPort(_self)
                .withAdminPort(_selfAdmin.getPort())
                .withExtensions(ImmutableMap.of("proxy", false))  // EndPoint is not a load-balancing proxy (eg. ELB)
                .build();

        return new ServiceEndPointBuilder()
                .withServiceName(serviceName)
                .withId(_self.toString())
                .withPayload(payload.toString())
                .build();
    }
}
