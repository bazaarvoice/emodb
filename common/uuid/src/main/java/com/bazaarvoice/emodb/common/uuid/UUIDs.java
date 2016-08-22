package com.bazaarvoice.emodb.common.uuid;

import java.util.UUID;

/**
 * Operations on UUIDs.
 */
public abstract class UUIDs {

    /**
     * Returns the byte-array equivalent of the specified UUID in big-endian order.
     */
    public static byte[] asByteArray(UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        byte[] buf = new byte[16];
        for (int i = 0; i < 8; i++) {
            buf[i]     = (byte) (msb >>> 8 * (7 - i));
            buf[i + 8] = (byte) (lsb >>> 8 * (7 - i));
        }
        return buf;
    }
}
