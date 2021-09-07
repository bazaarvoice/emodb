package com.bazaarvoice.emodb.web.throttling;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * Concurrent request regulator implementation which performs no throttling.
 */
public class UnthrottledConcurrentRequestRegulator implements ConcurrentRequestRegulator {

    private static final UnthrottledConcurrentRequestRegulator INSTANCE = new UnthrottledConcurrentRequestRegulator();

    private UnthrottledConcurrentRequestRegulator() {
        // empty
    }

    public static UnthrottledConcurrentRequestRegulator instance() {
        return INSTANCE;
    }

    @Override
    public void throttle(ContainerRequestContext request) {
        // no-op
    }

    @Override
    public void release(ContainerRequestContext request) {
        // no-op
    }
}
