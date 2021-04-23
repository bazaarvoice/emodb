package com.bazaarvoice.emodb.web.throttling;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This filter limits the number of concurrent requests based on the request parameters.  It is mostly a thin
 * wrapper with the necessary Jersey interfaces for calling {@link ConcurrentRequestRegulator#throttle(ContainerRequestContext)}
 * and {@link ConcurrentRequestRegulator#release(ContainerRequestContext)} respectively at the beginning and end of processing
 * a request.
 */
public class ConcurrentRequestsThrottlingFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private final ConcurrentRequestRegulatorSupplier _regulatorSupplier;

    /** Creates a new filter which regulates request throttling using the given supplier. */
    public ConcurrentRequestsThrottlingFilter(ConcurrentRequestRegulatorSupplier regulatorSupplier) {
        _regulatorSupplier = checkNotNull(regulatorSupplier, "Concurrent request regulator supplier is required");
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        _regulatorSupplier.forRequest(requestContext).throttle(requestContext);
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
        _regulatorSupplier.forRequest(requestContext).release(requestContext);
    }
}
