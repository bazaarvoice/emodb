package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class GracefulShutdownRegistry implements Managed {

    private final static Logger _log = LoggerFactory.getLogger(GracefulShutdownRegistry.class);

    private final Set<Closeable> _closeableWriters;
    private final Set<Closeable> _closeableWriteDependents;

    @Inject
    public GracefulShutdownRegistry(LifeCycleRegistry lifeCycleRegistry) {
        _closeableWriters = new HashSet<>();
        _closeableWriteDependents = new HashSet<>();
        requireNonNull(lifeCycleRegistry).manage(this);
    }

    @Override
    public void start() throws Exception { }

    @Override
    public void stop() throws Exception {
        _closeableWriters.forEach(writer -> {
            try {
                writer.close();
            } catch (IOException e) {
                _log.error("{} failed to close and shutdown all writes. There may be missing inconsistent data from" +
                        "services depending on write shutdown as a result", writer.getClass().getName());
            }
        });

        _closeableWriteDependents.forEach(writer -> {
            try {
                writer.close();
            } catch (IOException e) {
                _log.error("{} failed to close and finish its work. There may be missing inconsistent data as a result",
                        writer.getClass().getName());
            }
        });

    }

    public void registerWriter(Closeable closeableWriter) {
        _closeableWriters.add(closeableWriter);
    }

    public void registerWriteDependendent(Closeable closeableWriteDependent) {
        _closeableWriteDependents.add(closeableWriteDependent);
    }
}
