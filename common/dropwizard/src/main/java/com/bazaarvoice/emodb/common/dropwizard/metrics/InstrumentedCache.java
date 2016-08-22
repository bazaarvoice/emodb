package com.bazaarvoice.emodb.common.dropwizard.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.Cache;

/**
 * Registers Google Guava {@link Cache} objects with Dropwizard's {@link MetricRegistry}.
 */
public class InstrumentedCache {

    public static MetricsSet instrument(final Cache cache, MetricRegistry metricRegistry, String group, String scope, boolean includeLoadStatistics) {
        MetricsSet metrics = new MetricsSet(metricRegistry, group);

        Class<? extends Cache> type = cache.getClass();
        metrics.newGauge(type, "size", scope, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return cache.size();
            }
        });
        metrics.newGauge(type, "requests", scope, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return cache.stats().requestCount();
            }
        });
        metrics.newGauge(type, "hits", scope, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return cache.stats().hitCount();
            }
        });
        metrics.newGauge(type, "hit-rate", scope, new Gauge<Double>() {
            @Override
            public Double getValue() {
                return cache.stats().hitRate();
            }
        });
        metrics.newGauge(type, "misses", scope, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return cache.stats().missCount();
            }
        });
        metrics.newGauge(type, "miss-rate", scope, new Gauge<Double>() {
            @Override
            public Double getValue() {
                return cache.stats().missRate();
            }
        });
        metrics.newGauge(type, "evictions", scope, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return cache.stats().evictionCount();
            }
        });
        if (includeLoadStatistics) {
            metrics.newGauge(type, "loads", scope, new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return cache.stats().loadCount();
                }
            });
            metrics.newGauge(type, "load-successes", scope, new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return cache.stats().loadSuccessCount();
                }
            });
            metrics.newGauge(type, "load-exceptions", scope, new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return cache.stats().loadExceptionCount();
                }
            });
            metrics.newGauge(type, "load-exception-rate", scope, new Gauge<Double>() {
                @Override
                public Double getValue() {
                    return cache.stats().loadExceptionRate();
                }
            });
            metrics.newGauge(type, "average-load-penalty", scope, new Gauge<Double>() {
                @Override
                public Double getValue() {
                    return cache.stats().averageLoadPenalty();
                }
            });
        }

        return metrics;
    }
}
