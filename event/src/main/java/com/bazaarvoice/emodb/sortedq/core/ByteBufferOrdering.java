package com.bazaarvoice.emodb.sortedq.core;

import com.bazaarvoice.emodb.common.cassandra.nio.BufferUtils;
import com.google.common.collect.Ordering;

import java.nio.ByteBuffer;

/** Unsigned comparison of {@link ByteBuffer} objects. */
public class ByteBufferOrdering extends Ordering<ByteBuffer> {
    public static final ByteBufferOrdering INSTANCE = new ByteBufferOrdering();

    @Override
    public int compare(ByteBuffer left, ByteBuffer right) {
        return BufferUtils.compareUnsigned(left, right);
    }
}
