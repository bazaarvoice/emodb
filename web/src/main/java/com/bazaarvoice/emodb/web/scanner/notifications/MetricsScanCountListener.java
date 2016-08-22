package com.bazaarvoice.emodb.web.scanner.notifications;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;

/**
 * Scan count notifier that updates a metric with the active scan count.
 */
public class MetricsScanCountListener implements ScanCountListener {

    private final String ACTIVE_SCANS_METRIC_NAME = MetricRegistry.name("bv.emodb.scan", "ScanUploader", "active-scans");

    private int _active;

    @Inject
    public MetricsScanCountListener(MetricRegistry metricRegistry) {
        metricRegistry.register(ACTIVE_SCANS_METRIC_NAME, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return _active;
            }
        });
    }

    @Override
    public void pendingScanCountChanged(int pending) {
        // Don't send pending scan counts to Datadog
    }

    @Override
    public void activeScanCountChanged(int active) {
        _active = active;
    }
}
