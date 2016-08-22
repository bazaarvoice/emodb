package com.bazaarvoice.emodb.web.scanner.notifications;

import com.bazaarvoice.emodb.plugin.PluginServerMetadata;
import com.bazaarvoice.emodb.plugin.stash.StashMetadata;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.setup.Environment;

import java.util.Date;

/**
 * Stash state listener that updates metrics.
 */
public class MetricsStashStateListener implements StashStateListener<Void> {

    private Meter _stashParticipantMeter;
    private Meter _stashStartedMeter;
    private Meter _stashCompletedMeter;
    private Meter _tashCanceledMeter;

    @Override
    public void init(Environment environment, PluginServerMetadata pluginServerMetadata, Void ignore) {
        MetricRegistry metricRegistry = environment.metrics();
        _stashParticipantMeter = metricRegistry.meter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "scan-started"));
        _stashStartedMeter = metricRegistry.meter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "scan-participant"));
        _stashCompletedMeter = metricRegistry.meter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "scan-completed"));
        _tashCanceledMeter = metricRegistry.meter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "scan-canceled"));
    }

    @Override
    public void announceStashParticipation() {
        _stashParticipantMeter.mark();
    }

    @Override
    public void stashStarted(StashMetadata info) {
        _stashStartedMeter.mark();
    }

    @Override
    public void stashCompleted(StashMetadata info, Date completeTime) {
        _stashCompletedMeter.mark();
    }

    @Override
    public void stashCanceled(StashMetadata info, Date canceledTime) {
        _tashCanceledMeter.mark();
    }
}
