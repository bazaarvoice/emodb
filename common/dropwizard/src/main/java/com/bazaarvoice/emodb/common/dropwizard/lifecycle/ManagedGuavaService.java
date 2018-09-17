package com.bazaarvoice.emodb.common.dropwizard.lifecycle;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.Service;
import io.dropwizard.lifecycle.Managed;

/**
 * Adapts a Guava {@link Service} to Dropwizard start and stop events.
 * <p>
 * Note that Guava services are not restartable.  The {@link #start()} method may not be called after {@link #stop()}.
 */
public class ManagedGuavaService implements Managed {
    private final Service _service;

    public ManagedGuavaService(Service service) {
        _service = service;
    }

    @Override
    public void start() throws Exception {
        _service.startAsync().awaitRunning();
    }

    @Override
    public void stop() throws Exception {
        _service.stopAsync().awaitTerminated();
    }

    // For debugging
    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(_service).toString();
    }
}
