package com.bazaarvoice.emodb.databus.core;

import com.google.common.base.Charsets;

/**
 * Interface for getting the partition for an event.  All partitions numbers are in the range of [0..n-1], where n
 * is the maximum number of available partitions.
 */
public interface PartitionSelector {

    int getPartition(byte[] key);

    /**
     * Convenience implementation for getting the partition for a String.
     */
    default int getPartition(String key) {
        return key != null ? getPartition(key.getBytes(Charsets.UTF_8)) : 0;
    }

    /**
     * Efficient default implementation where there is only one partition.
     */
    PartitionSelector SINGLE_PARTITION_SELECTOR = key -> 0;
}
