package com.bazaarvoice.emodb.web.throttling;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import com.sun.jersey.spi.container.ResourceFilter;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This filter limits the number of concurrent requests based on the request parameters.  It is mostly a thin
 * wrapper with the necessary Jersey interfaces for calling {@link ConcurrentRequestRegulator#throttle(ContainerRequest)}
 * and {@link ConcurrentRequestRegulator#release(ContainerRequest)} respectively at the beginning and end of processing
 * a request.
 */
public class ConcurrentRequestsThrottlingFilter implements ResourceFilter, ContainerRequestFilter, ContainerResponseFilter {

    private final ConcurrentRequestRegulatorSupplier _regulatorSupplier;

    /** Creates a new filter which regulates request throttling using the given supplier. */
    public ConcurrentRequestsThrottlingFilter(ConcurrentRequestRegulatorSupplier regulatorSupplier) {
        _regulatorSupplier = checkNotNull(regulatorSupplier, "Concurrent request regulator supplier is required");
    }

    @Override
    public ContainerRequest filter(ContainerRequest request) {
        _regulatorSupplier.forRequest(request).throttle(request);
        return request;
    }

    @Override
    public ContainerResponse filter(ContainerRequest request, ContainerResponse response) {
        _regulatorSupplier.forRequest(request).release(request);
        return response;
    }

    @Override
    public ContainerRequestFilter getRequestFilter() {
        return this;
    }

    @Override
    public ContainerResponseFilter getResponseFilter() {
        return this;
    }
}
