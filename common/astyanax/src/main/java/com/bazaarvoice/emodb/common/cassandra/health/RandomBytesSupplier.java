package com.bazaarvoice.emodb.common.cassandra.health;

import com.google.common.base.Supplier;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Provides randomized byte buffer row keys for health checks.  The distribution of keys is such that it will
 * spread health checks across across the entire ring whether the ring is using the RandomPartitioner or the
 * ByteOrderedPartitioner.  However, the byte buffers aren't likely to be compatible with column families that
 * validate that the rows are valid strings or dates or other specific types.
 */
public class RandomBytesSupplier implements Supplier<ByteBuffer> {
    private final Random _random = new Random();

    @Override
    public ByteBuffer get() {
        byte[] bytes = new byte[16];
        _random.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }
}
