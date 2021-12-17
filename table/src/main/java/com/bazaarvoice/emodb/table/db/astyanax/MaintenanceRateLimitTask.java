package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Shows or changes the current per-Cassandra cluster rate limits for background data maintenance operations
 * purge and copy.  Rate limits are in operations per second.
 * <p>
 * To display the rate limits currently in effect:
 * <pre>
 *   curl -s -XPOST "http://localhost:8081/tasks/sor-rate-limits"
 * </pre>
 * To modify a rate limit for a specific Cassandra cluster, specify the cluster name and the operations per second,
 * for example:
 * <pre>
 *   curl -s -XPOST "http://localhost:8081/tasks/sor-rate-limits?ci_sor_cat_default=2.5"
 * </pre>
 */
public class MaintenanceRateLimitTask extends Task {
    private final MapStore<Double> _rateLimits;
    private final RateLimiterCache _rateLimiterCache;
    private final List<String> _clusters;

    @Inject
    public MaintenanceRateLimitTask(TaskRegistry taskRegistry, @Maintenance String scope,
                                    @Maintenance MapStore<Double> rateLimits,
                                    @Maintenance RateLimiterCache rateLimiterCache,
                                    @KeyspaceMap Map<String, CassandraKeyspace> keyspaceMap) {
        super(scope + "-rate-limits");
        _rateLimits = requireNonNull(rateLimits, "rateLimits");
        _rateLimiterCache = requireNonNull(rateLimiterCache, "rateLimiterCache");
        _clusters = Ordering.natural().immutableSortedCopy(Sets.newHashSet(
                Iterables.transform(keyspaceMap.values(), new Function<CassandraKeyspace, String>() {
                    @Override
                    public String apply(CassandraKeyspace keyspace) {
                        return keyspace.getClusterName();
                    }
                })));
        taskRegistry.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter out) throws Exception {
        // Modify rate limits.
        if (!parameters.isEmpty()) {
            for (Map.Entry<String, String> entry : parameters.entries()) {
                String cluster = entry.getKey();
                double rate = Double.parseDouble(entry.getValue());
                checkArgument(_clusters.contains(cluster), "Unknown cluster: %s", cluster);
                // App only observers rate limit changes when it makes progress and if the limit is too low it'll never make progress.
                checkArgument(rate >= 1/60.0, "Rate limit too small, must be at least once per minute: %s", rate);
                _rateLimits.set(cluster, rate);
            }
            Thread.sleep(500);  // Wait for values to round trip through ZooKeeper.
        }

        // Print the current settings.
        for (String cluster : _clusters) {
            out.printf("%s: %,g%n", cluster, _rateLimiterCache.get(cluster).getRate());
        }
    }
}
