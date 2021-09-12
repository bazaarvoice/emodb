package com.bazaarvoice.emodb.table.db.consistency;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkDurationSerializer;
import com.bazaarvoice.emodb.table.db.astyanax.Maintenance;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;

import java.time.Duration;
import java.util.Map;

/**
 * Dropwizard task to update the full consistency minimum lag duration values for each cluster.
 * <p>
 * Usage:
 * <pre>
 *     # For blob store, replace sor-compaction-lag with blob-compaction-lag
 *     # View the current settings
 *     curl -s -XPOST http://localhost:8081/tasks/sor-compaction-lag
 *
 *     # Modify the current settings for all clusters with a new value, eg. 30 minutes
 *     curl -s -XPOST 'http://localhost:8081/tasks/sor-compaction-lag?all=PT30M'
 *
 *     # Modify the current settings for a single cluster with a new value, eg. 12 hours
 *     curl -s -XPOST 'http://localhost:8081/tasks/sor-compaction-lag?emo_cluster=PT12H'
 *
 *     # Reset settings back to default values
 *     curl -s -XPOST 'http://localhost:8081/tasks/sor-compaction-lag?all='
 * </pre>
 */
public class MinLagDurationTask extends ConsistencyControlTask<Duration> {
    @Inject
    public MinLagDurationTask(TaskRegistry taskRegistry,
                              @Maintenance String scope,
                              @GlobalFullConsistencyZooKeeper CuratorFramework curator,
                              @MinLagDurationValues Map<String, ValueStore<Duration>> durationCache) {
        super(taskRegistry, scope + "-compaction-lag", "Full consistency minimum lag",
                durationCache, curator, new ZkDurationSerializer(),
                Suppliers.ofInstance(MinLagConsistencyTimeProvider.DEFAULT_LAG));
    }

    @Override
    protected String toString(Duration duration) {
        return super.toString(duration) + " (" + duration.toMillis() + "ms)";
    }
}
