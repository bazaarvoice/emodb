package com.bazaarvoice.emodb.common.dropwizard.lifecycle;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches a Guava service and logs when it fails, usually because it threw an uncaught exception
 * that bubbled out of the main service run method.
 */
public class ServiceFailureListener extends Service.Listener {
    private static final Logger _log = LoggerFactory.getLogger(ServiceFailureListener.class);

    private final Meter _failureMeter;

    private final Service _service;

    public static void listenTo(Service service, MetricRegistry metricRegistry) {
        service.addListener(new ServiceFailureListener(service, metricRegistry), MoreExecutors.directExecutor());
    }

    public ServiceFailureListener(Service service, MetricRegistry metricRegistry) {
        _service = service;

        _failureMeter = metricRegistry.meter(MetricRegistry.name("bv.emodb.service", "StateTransition", "failed"));
    }

    @Override
    public void starting() {
        // Do nothing
    }

    @Override
    public void running() {
        // Do nothing
    }

    @Override
    public void stopping(Service.State from) {
        // Do nothing
    }

    @Override
    public void terminated(Service.State from) {
        // Do nothing
    }

    @Override
    public void failed(Service.State from, Throwable failure) {
        _log.error("Service {} has failed while in the {} state.", _service.toString(), from, failure);
        _failureMeter.mark();
    }
}
