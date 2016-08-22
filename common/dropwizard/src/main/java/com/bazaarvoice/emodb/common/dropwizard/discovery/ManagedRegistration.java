package com.bazaarvoice.emodb.common.dropwizard.discovery;

import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceRegistry;
import com.google.common.base.Objects;
import io.dropwizard.lifecycle.Managed;

/**
 * Dropwizard managed registration of an SOA (Ostrich) end point.
 */
public class ManagedRegistration implements Managed {
    private final ServiceRegistry _serviceRegistry;
    private final ServiceEndPoint _endPoint;

    public ManagedRegistration(ServiceRegistry serviceRegistry, ServiceEndPoint endPoint) {
        _serviceRegistry = serviceRegistry;
        _endPoint = endPoint;
    }

    @Override
    public void start() throws Exception {
        _serviceRegistry.register(_endPoint);
    }

    @Override
    public void stop() throws Exception {
        _serviceRegistry.unregister(_endPoint);
    }

    // For debugging
    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(_endPoint).toString();
    }
}
