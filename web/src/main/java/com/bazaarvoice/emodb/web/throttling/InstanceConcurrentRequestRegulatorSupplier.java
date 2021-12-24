package com.bazaarvoice.emodb.web.throttling;

import com.sun.jersey.spi.container.ContainerRequest;

import static java.util.Objects.requireNonNull;

/**
 * Supplies a single instance of a concurrent request regulator for all requests.
 */
public class InstanceConcurrentRequestRegulatorSupplier implements ConcurrentRequestRegulatorSupplier {

    private final ConcurrentRequestRegulator _requestRegulator;

    public InstanceConcurrentRequestRegulatorSupplier(ConcurrentRequestRegulator requestRegulator) {
        _requestRegulator = requireNonNull(requestRegulator, "Request regulator is required");
    }

    @Override
    public ConcurrentRequestRegulator forRequest(ContainerRequest request) {
        return _requestRegulator;
    }
}
