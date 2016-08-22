package com.bazaarvoice.emodb.sor.log;

import com.bazaarvoice.emodb.sor.core.Expanded;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * Logs records with too many uncompacted deltas to a Logback {@link Logger}.
 */
public class LogbackSlowQueryLog implements SlowQueryLog {
    private final Logger _logger;
    private final int _tooManyDeltasThreshold;
    private final Meter _meter;

    public LogbackSlowQueryLog(Logger logger, int tooManyDeltasThreshold, MetricRegistry metricRegistry) {
        _logger = logger;
        _tooManyDeltasThreshold = tooManyDeltasThreshold;
        _meter = metricRegistry.meter(MetricRegistry.name("bv.emodb.sor", "SlowQueryLog", "too_many_deltas"));
    }

    @Override
    public void log(String table, String key, Expanded expanded) {
        if (expanded.getNumPersistentDeltas() >= _tooManyDeltasThreshold) {
            _logger.info("Too many deltas: {} {} {} {}",
                    expanded.getNumPersistentDeltas(), expanded.getNumDeletedDeltas(), encode(table), encode(key));
            _meter.mark();
        }
    }

    private String encode(String table) {
        try {
            // Don't need to escape ':' and it's common, so for cleaner output preserve ':'
            return URLEncoder.encode(table, "UTF-8").replace("%3A", ":");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
