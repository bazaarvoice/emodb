package com.bazaarvoice.megabus.resolver;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Watches the MegabusRefResolverManager service.
 * Main implementation is on failure to restart the service.
 * One failure case is when the service's new state gets to NOT RUNNING through the ERROR state.
 * Its also when an uncaught exception get bubbled out of the main service run method.
 */
public class MegabusRefResolveFailureListener extends Service.Listener {
    private static final Logger _log = LoggerFactory.getLogger(MegabusRefResolveFailureListener.class);
    private final static long RESTART_SLEEP_TIME_IN_MS = Duration.ofMinutes(1).toMillis();

    private final Meter _restartMeter;

    private final Service _service;

    private final String _applicationId;

    public static void listenTo(Service service, String applicationId, MetricRegistry metricRegistry) {
        service.addListener(new MegabusRefResolveFailureListener(service, applicationId, metricRegistry), MoreExecutors.sameThreadExecutor());
    }

    public MegabusRefResolveFailureListener(Service service, String applicationId, MetricRegistry metricRegistry) {
        _service = checkNotNull(service);
        _applicationId = checkNotNull(applicationId);
        _restartMeter = metricRegistry.meter(MegabusRefResolver.getMetricName("restart" + _service));
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
        _log.error("Service {} with applicationId {} has failed while in the {} state.", _service.toString(), _applicationId + "-resolver", from, failure);

        _service.stopAsync();
        _service.awaitTerminated();

        try {
            Thread.sleep(RESTART_SLEEP_TIME_IN_MS);
        } catch (InterruptedException e) {
            _log.error("sleep is interrupted: ", e.getMessage());
        }

        // update the metrics
        _restartMeter.mark();

        _log.info("Restarting service {} at: ", _service.toString(), LocalDateTime.now());
        _service.startAsync();
    }
}
