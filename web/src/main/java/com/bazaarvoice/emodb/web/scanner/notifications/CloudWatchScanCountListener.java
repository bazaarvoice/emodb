package com.bazaarvoice.emodb.web.scanner.notifications;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Scan count notifier that publishes the total active and pending scan counts as a CloudWatch metric.
 */
public class CloudWatchScanCountListener extends AbstractScheduledService implements ScanCountListener {

    private final Logger _log = LoggerFactory.getLogger(CloudWatchScanCountListener.class);

    private static final String NAMESPACE = "EmoDB/Scanner";
    private static final String ACTIVE_AND_PENDING_SCANS = "ActiveAndPendingScans";

    private final AmazonCloudWatch _cloudWatch;
    private final List<Dimension> _dimensions;
    private volatile int _activeScanCount;
    private volatile int _pendingScanCount;

    @Inject
    public CloudWatchScanCountListener(AmazonCloudWatch cloudWatch, List<Dimension> dimensions, LifeCycleRegistry lifecycle) {
        _cloudWatch = cloudWatch;
        _dimensions = dimensions;
        lifecycle.manage(new ManagedGuavaService(this));
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(30, 30, TimeUnit.SECONDS);
    }

    @Override
    protected void runOneIteration() throws Exception {
        try {
            _cloudWatch.putMetricData(
                    new PutMetricDataRequest()
                            .withNamespace(NAMESPACE)
                            .withMetricData(
                                    new MetricDatum()
                                            .withTimestamp(new Date())
                                            .withMetricName(ACTIVE_AND_PENDING_SCANS)
                                            .withValue((double) (_activeScanCount + _pendingScanCount))
                                            .withUnit(StandardUnit.Count)
                                            .withDimensions(_dimensions)));
        } catch (AmazonClientException e) {
            _log.error("Failed to publish active and pending scans metric", e);
        }
    }

    @Override
    public void pendingScanCountChanged(int pending) {
        _pendingScanCount = pending;
    }

    @Override
    public void activeScanCountChanged(int active) {
        _activeScanCount = active;
    }
}
