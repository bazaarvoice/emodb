package com.bazaarvoice.emodb.sor.db.astyanax;

import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.util.ByteBufferRangeImpl;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

/**
 * Converts the start and end of a {@code ByteBufferRange} to and from a String.
 * Ignores the reverse and limit fields.
 */
class SplitFormat {
    /**
     * Returns a string that looks like "commonPrefix:startSuffix-endSuffix", hex-encoding the bytes.
     */
    static String encode(ByteBufferRange range) {
        String start = Hex.encodeHexString(toArray(range.getStart()));
        String end = Hex.encodeHexString(toArray(range.getEnd()));
        int prefix = getCommonPrefixLength(start, end);
        return start.substring(0, prefix) + ':' + start.substring(prefix) + '-' + end.substring(prefix);
    }

    /**
     * Parses a hex string that looks like "commonPrefix:startSuffix-endSuffix".
     */
    static ByteBufferRange decode(String string) {
        int prefix = string.indexOf(':');
        int sep = string.indexOf('-', prefix + 1);
        checkArgument(prefix >= 0 && sep >= 0, "Invalid split string: %s", string);

        char[] start = new char[prefix + sep - (prefix + 1)];
        string.getChars(0, prefix, start, 0);
        string.getChars(prefix + 1, sep, start, prefix);

        char[] end = new char[prefix + string.length() - (sep + 1)];
        string.getChars(0, prefix, end, 0);
        string.getChars(sep + 1, string.length(), end, prefix);

        byte[] startBytes, endBytes;
        try {
            startBytes = Hex.decodeHex(start);
            endBytes = Hex.decodeHex(end);
        } catch (DecoderException e) {
            throw new IllegalArgumentException(format("Invalid split string: %s", string));
        }

        return new ByteBufferRangeImpl(ByteBuffer.wrap(startBytes), ByteBuffer.wrap(endBytes), -1, false);
    }

    private static int getCommonPrefixLength(String s1, String s2) {
        int max = Math.min(s1.length(), s2.length());
        for (int i = 0; i < max; i++) {
            if (s1.charAt(i) != s2.charAt(i)) {
                return i;
            }
        }
        return max;
    }

    private static byte[] toArray(ByteBuffer buf) {
        if (buf.hasArray() && buf.arrayOffset() == 0 && buf.position() == 0 && buf.limit() == buf.capacity()) {
            return buf.array();
        } else {
            byte[] bytes = new byte[buf.remaining()];
            buf.duplicate().get(bytes);
            return bytes;
        }
    }
}
