package com.bazaarvoice.emodb.web.throttling;

import javax.ws.rs.container.ContainerRequestContext;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Supplies a single instance of a concurrent request regulator for all requests.
 */
public class InstanceConcurrentRequestRegulatorSupplier implements ConcurrentRequestRegulatorSupplier {

    private final ConcurrentRequestRegulator _requestRegulator;

    public InstanceConcurrentRequestRegulatorSupplier(ConcurrentRequestRegulator requestRegulator) {
        _requestRegulator = checkNotNull(requestRegulator, "Request regulator is required");
    }

    @Override
    public ConcurrentRequestRegulator forRequest(ContainerRequestContext request) {
        return _requestRegulator;
    }
}
