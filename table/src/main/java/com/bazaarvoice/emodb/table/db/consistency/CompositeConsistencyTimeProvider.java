package com.bazaarvoice.emodb.table.db.consistency;

import com.bazaarvoice.emodb.table.db.ClusterInfo;
import com.bazaarvoice.emodb.table.db.astyanax.FullConsistencyTimeProvider;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Combines the effect of {@link HintsConsistencyTimeProvider} and {@link MinLagConsistencyTimeProvider}.
 */
public class CompositeConsistencyTimeProvider implements FullConsistencyTimeProvider {
    /** Hard-coded mininum lag that may not be exceeded. */
    private static final Duration FIXED_MINIMUM_LAG = Duration.ofMinutes(1);

    /** Hard-coded maximum lag that may not be exceeded. */
    private static final Duration FIXED_MAXIMUM_LAG = Duration.ofDays(10);

    private final List<FullConsistencyTimeProvider> _providers;

    @Inject
    public CompositeConsistencyTimeProvider(Collection<ClusterInfo> clusterInfo,
                                            List<FullConsistencyTimeProvider> providers,
                                            MetricRegistry metricRegistry) {
        requireNonNull(clusterInfo, "clusterInfo");
        _providers = requireNonNull(providers, "providers");

        // Publish metrics for the full consistency timestamp as a time lag compared to now.  It's easier to alert
        // on a time lag that exceeds min/max bounds compared to alerting on the timestamp itself.
        for (final ClusterInfo cluster : clusterInfo) {
            // Idempotent. Adds the gauge metric. If it already exists, then it gets the existing gauge.
            String metricName = MetricRegistry.name("bv.emodb.sor", "FullConsistencyTimeProvider", "lag-" + cluster.getClusterMetric());
            if (!metricRegistry.getGauges().containsKey(metricName)) {
                metricRegistry.register(metricName,
                        new Gauge<Double>() {
                            @Override
                            public Double getValue() {
                                long timestamp = getMaxTimeStamp(cluster.getCluster());
                                long lag = System.currentTimeMillis() - timestamp;
                                return lag / 1000.0; // convert millis to seconds
                            }
                        });
            }
        }
    }

    @Override
    public long getMaxTimeStamp(String cluster) {
        long now = System.currentTimeMillis();

        // Pick the oldest timestamp of the configured providers.
        long maxTimestamp = now;
        for (FullConsistencyTimeProvider provider : _providers) {
            maxTimestamp = Math.min(maxTimestamp, provider.getMaxTimeStamp(cluster));
        }

        // Enforce hard-coded minimum and maximum lag values.
        maxTimestamp = Math.max(maxTimestamp, now - FIXED_MAXIMUM_LAG.toMillis());
        maxTimestamp = Math.min(maxTimestamp, now - FIXED_MINIMUM_LAG.toMillis());

        return maxTimestamp;
    }
}
