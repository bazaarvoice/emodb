package com.bazaarvoice.emodb.common.dropwizard.lifecycle;

import com.google.common.base.Objects;
import com.google.common.io.Closeables;
import io.dropwizard.lifecycle.Managed;

import java.io.Closeable;

/**
 * Adapts the Dropwizard {@link Managed} interface for a {@link Closeable}.  This allows Dropwizard to
 * cleanup resources without those servers requiring a direct dependency on Dropwizard.
 */
public class ManagedCloseable implements Managed {
    private final Closeable _closeable;

    public ManagedCloseable(Closeable closeable) {
        _closeable = closeable;
    }

    @Override
    public void start() throws Exception {
        // do nothing
    }

    @Override
    public void stop() throws Exception {
        Closeables.close(_closeable, true);
    }

    // For debugging
    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(_closeable).toString();
    }
}
