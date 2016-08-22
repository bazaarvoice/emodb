package com.bazaarvoice.emodb.web.throttling;

import com.sun.jersey.spi.container.ContainerRequest;

/**
 * Defines how throttling should be applied to a {@link ContainerRequest}.
 */
public interface ConcurrentRequestRegulator {

    /**
     * If the request has been throttled this method should raise a WebApplicationException which accurately
     * reflects that the throttle has been applied.  If the request is not throttled this method should return
     * normally to allow continued request processing.
     */
    void throttle(ContainerRequest request);

    /**
     * Every request which was throttled by {@link #throttle(ContainerRequest)} must be followed by a subsequent call
     * to this method once the request is complete.  If this method receives a request which was not throttled it
     * should return normally.
     */
    void release(ContainerRequest request);
}
