package com.bazaarvoice.emodb.web.throttling;

import com.sun.jersey.spi.container.ContainerRequest;

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
    public void throttle(ContainerRequest request) {
        // no-op
    }

    @Override
    public void release(ContainerRequest request) {
        // no-op
    }
}
