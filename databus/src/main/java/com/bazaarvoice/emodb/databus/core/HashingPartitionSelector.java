package com.bazaarvoice.emodb.databus.core;

import com.google.common.hash.Hashing;

/**
 * {@link PartitionSelector} which determines the partition based on a hash of the input value.
 */
public class HashingPartitionSelector implements PartitionSelector {

    private final int _numPartitions;

    public HashingPartitionSelector(int numPartitions) {
        _numPartitions = numPartitions;
    }

    @Override
    public int getPartition(byte[] key) {
        return Math.abs(Hashing.murmur3_32().hashBytes(key).asInt()) % _numPartitions;
    }
}
