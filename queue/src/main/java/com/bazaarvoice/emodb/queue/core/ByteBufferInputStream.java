package com.bazaarvoice.emodb.queue.core;

import java.io.InputStream;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkPositionIndexes;
import static java.util.Objects.requireNonNull;

/**
 * Wraps a {@link java.nio.ByteBuffer} in an {@link java.io.InputStream}.
 * <p>
 * This will consume the remaining bytes in the byte buffer.  If you don't want to consume the bytes,
 * call {@code buffer.duplicate()} when constructing the {@code ByteBufferInputStream}.
 */
public class ByteBufferInputStream extends InputStream {
    private final ByteBuffer _buffer;

    public ByteBufferInputStream(ByteBuffer buffer) {
        _buffer = buffer;
    }

    @Override
    public int available() {
        return _buffer.remaining();
    }

    @Override
    public int read() {
        if (!_buffer.hasRemaining()) {
            return -1;
        }
        return _buffer.get() & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        requireNonNull(b);
        checkPositionIndexes(off, off + len, b.length);
        if (len == 0) {
            return 0;
        }
        if (!_buffer.hasRemaining()) {
            return -1;
        }
        int count = Math.min(len, _buffer.remaining());
        _buffer.get(b, off, count);
        return count;
    }

    @Override
    public long skip(long n) {
        long count = Math.min(n, _buffer.remaining());
        if (count <= 0) {
            return 0;
        }
        _buffer.position(_buffer.position() + (int) count);
        return count;
    }
}
