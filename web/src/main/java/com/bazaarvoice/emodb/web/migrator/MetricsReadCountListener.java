package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.web.scanner.notifications.ScanCountListener;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;

public class MetricsReadCountListener implements ScanCountListener{

    private final String ACTIVE_SCANS_METRIC_NAME = MetricRegistry.name("bv.emodb.scan", "ScanUploader", "active-scans");

    private int _active;

    @Inject
    public MetricsReadCountListener(MetricRegistry metricRegistry) {
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
