package com.bazaarvoice.emodb.common.cassandra.nio;

import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class BufferUtils {

    /**
     * Converts all remaining bytes in the buffer a String using the specified encoding.
     * Does not move the buffer position.
     */
    public static String getString(ByteBuffer buf, Charset encoding) {
        return getString(buf, 0, buf.remaining(), encoding);
    }

    /**
     * Converts the specified number of bytes in the buffer and converts them to a String using
     * the specified encoding.  Does not move the buffer position.
     */
    public static String getString(ByteBuffer buf, int offset, int length, Charset encoding) {
        buf = buf.duplicate();
        buf.position(buf.position() + offset);
        if (buf.hasArray()) {
            return new String(buf.array(), buf.arrayOffset() + buf.position(), length, encoding);
        } else {
            byte[] bytes = new byte[length];
            buf.get(bytes);
            return new String(bytes, encoding);
        }
    }

    public static int indexOf(ByteBuffer buf, byte b) {
        for (int i = 0; i < buf.remaining(); i++) {
            if (buf.get(buf.position() + i) == b) {
                return i;
            }
        }
        return -1;
    }

    public static int compareUnsigned(ByteBuffer o1, ByteBuffer o2)
    {
        assert o1 != null;
        assert o2 != null;
        if (o1 == o2)
            return 0;

        if (!o1.isReadOnly() && !o2.isReadOnly())
        {
            // We can delegate to C* 2.2.4 if both buffers are non-read-only.
            // If either one of them is read only, Cassandra 2.2.4 gives a segfault.
            return ByteBufferUtil.compareUnsigned(o1, o2);
        }

        int end1 = o1.position() + o1.remaining();
        int end2 = o2.position() + o2.remaining();
        for (int i = o1.position(), j = o2.position(); i < end1 && j < end2; i++, j++)
        {
            int a = (o1.get(i) & 0xff);
            int b = (o2.get(j) & 0xff);
            if (a != b)
                return a - b;
        }
        return o1.remaining() - o2.remaining();
    }
}
