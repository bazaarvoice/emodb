package com.bazaarvoice.emodb.common.dropwizard.lifecycle;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.dropwizard.lifecycle.Managed;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Simple implementation of {@link LifeCycleRegistry}.
 */
public class SimpleLifeCycleRegistry implements LifeCycleRegistry, Managed, Closeable {
    private final List<Managed> _managed = Lists.newArrayList();

    @Override
    public void start() throws Exception {
        for (Managed managed : _managed) {
            managed.start();
        }
    }

    @Override
    public void stop() throws Exception {
        for (Managed managed : Lists.reverse(_managed)) {
            managed.stop();
        }
        _managed.clear();
    }

    @Override
    public void close() throws IOException {
        try {
            stop();
        } catch (Exception e) {
            Throwables.propagateIfInstanceOf(e, IOException.class);
            Throwables.propagateIfPossible(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T extends Managed> T manage(T managed) {
        _managed.add(managed);
        return managed;
    }

    @Override
    public <T extends Closeable> T manage(T closeable) {
        _managed.add(new ManagedCloseable(closeable));
        return closeable;
    }
}
