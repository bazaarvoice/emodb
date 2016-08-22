package com.bazaarvoice.emodb.table.db.consistency;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkTimestampSerializer;
import com.bazaarvoice.emodb.table.db.astyanax.Maintenance;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.joda.time.Duration;

import java.util.Map;

/**
 * Dropwizard task to update the full consistency max timesetamp values for each cluster.
 * <p>
 * Usage:
 * <pre>
 *     # View the current settings
 *     # For blob store, replace sor-compaction-timestamp with blob-compaction-timestamp
 *     curl -s -XPOST http://localhost:8081/tasks/sor-compaction-timestamp
 *
 *     # Modify the current settings for all clusters with a new milliseconds value
 *     # (eg. "1382620121882" for 2013-10-24 13:08:41.882 UTC)
 *     curl -s -XPOST 'http://localhost:8081/tasks/sor-compaction-timestamp?all=PT30M'
 *
 *     # Modify the current settings for a single cluster with a new milliseconds value
 *     # (eg. "1382620121882" for 2013-10-24 13:08:41.882 UTC)
 *     curl -s -XPOST 'http://localhost:8081/tasks/sor-compaction-timestamp?emo_cluster=1382620121882'
 *
 *     # Modify the current setting to a specific timestamp
 *     curl -s -XPOST 'http://localhost:8081/tasks/sor-compaction-timestamp?all=2013-10-24T13:08:41.882Z'
 *
 *     # Reset settings back to default values
 *     curl -s -XPOST 'http://localhost:8081/tasks/sor-compaction-timestamp?all='
 * </pre>
 */
public class HintsConsistencyTimeTask extends ConsistencyControlTask<Long> {
    @Inject
    public HintsConsistencyTimeTask(TaskRegistry taskRegistry, @Maintenance String scope,
                                    @GlobalFullConsistencyZooKeeper CuratorFramework curator,
                                    @HintsConsistencyTimeValues Map<String, ValueStore<Long>> timestampCache) {
        super(taskRegistry, scope + "-compaction-timestamp", "Full consistency maximum timestamp",
                timestampCache, curator, new ZkTimestampSerializer(),
                new Supplier<Long>() {
                    @Override
                    public Long get() {
                        return HintsConsistencyTimeProvider.getDefaultTimestamp();
                    }
                });
    }

    @Override
    protected String toString(Long value) {
        return super.toString(value) + " (" + Duration.millis(System.currentTimeMillis() - value).toPeriod() + ")";
    }
}
