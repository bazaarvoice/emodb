package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.sor.audit.AuditFlusher;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Because Emo persists audits in batches, it is necessary to ensure that they all get persisted when Emo shuts down.
 * This class leverages Dropwizard's {@link Managed} API to facilitate this process during shutdown.
 */
public class GracefulShutdownManager implements Managed {

    private final static Logger _log = LoggerFactory.getLogger(GracefulShutdownManager.class);

    private final DataWriteCloser _dataWriteCloser;
    private final AuditFlusher _auditFlusher;

    @Inject
    public GracefulShutdownManager(AuditFlusher auditFlusher, DataWriteCloser dataWriteCloser, LifeCycleRegistry lifeCycleRegistry) {
        _dataWriteCloser = dataWriteCloser;
        _auditFlusher = auditFlusher;
        requireNonNull(lifeCycleRegistry).manage(this);
    }

    @Override
    public void start() throws Exception { }

    @Override
    public void stop() throws Exception {
        _dataWriteCloser.closeWrites();
        _auditFlusher.flushAndShutdown();
    }
}
