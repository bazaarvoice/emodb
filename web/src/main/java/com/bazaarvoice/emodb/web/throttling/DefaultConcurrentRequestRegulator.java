package com.bazaarvoice.emodb.web.throttling;

import com.codahale.metrics.Meter;
import com.google.common.base.Strings;

import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import java.util.concurrent.Semaphore;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Default implementation for {@link ConcurrentRequestRegulator} which uses a Java semaphore to control concurrency.
 * The semaphore is stored as a property on the request to guarantee that the same semaphore that was acquired is
 * always released.
 */
public class DefaultConcurrentRequestRegulator implements ConcurrentRequestRegulator {

    private final String _semaphoreProperty;
    private final Semaphore _semaphore;
    private final Meter _throttlingMeter;

    public DefaultConcurrentRequestRegulator(String semaphoreProperty, int maxConcurrentRequests,
                                             @Nullable Meter throttlingMeter) {
        checkArgument(!Strings.isNullOrEmpty(semaphoreProperty), "Semaphore property cannot be null or empty");
        checkArgument(maxConcurrentRequests >= 0, "Max concurrent requests cannot be negative");
        _semaphoreProperty = semaphoreProperty;
        _semaphore = new Semaphore(maxConcurrentRequests);
        _throttlingMeter = throttlingMeter;
    }

    @Override
    public void throttle(ContainerRequestContext request) {
        if (!_semaphore.tryAcquire()) {
            if (_throttlingMeter != null) {
                _throttlingMeter.mark();
            }
            String response = String.format("Too many concurrent requests for %s. Try again later.", request.getUriInfo().getPath());
            throw new WebApplicationException(Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(response).build());
        }
        request.setProperty(_semaphoreProperty, _semaphore);
    }

    @Override
    public void release(ContainerRequestContext request) {
        Semaphore semaphore = (Semaphore) request.getProperty(_semaphoreProperty);
        if (semaphore != null) {
            semaphore.release();
        }
    }
}