package com.bazaarvoice.emodb.common.zookeeper.leader;

import com.google.common.util.concurrent.Service;

/**
 * Interface used by {@link PartitionedLeaderService} to get the service for handling a partition.
 * Partition numbers are guaranteed to be in the range [0..numPartitions-1]
 */
public interface PartitionedServiceSupplier {

    Service createForPartition(int partition);
}
