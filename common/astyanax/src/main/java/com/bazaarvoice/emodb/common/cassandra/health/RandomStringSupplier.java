package com.bazaarvoice.emodb.common.cassandra.health;

import com.google.common.base.Supplier;
import com.netflix.astyanax.serializers.StringSerializer;

import java.nio.ByteBuffer;

/**
 * Provides randomized string row keys for health checks.  The distribution of keys is such that it will
 * spread health checks across across the entire ring when the ring is using the RandomPartitioner.
 */
public class RandomStringSupplier implements Supplier<ByteBuffer> {

    @Override
    public ByteBuffer get() {
        // A random double is guaranteed to be a valid ascii and/or utf8 string.
        return StringSerializer.get().toByteBuffer(Double.toString(Math.random()));
    }
}
