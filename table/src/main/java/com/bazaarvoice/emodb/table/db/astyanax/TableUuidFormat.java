package com.bazaarvoice.emodb.table.db.astyanax;

import com.google.common.primitives.UnsignedLongs;

public class TableUuidFormat {

    public static String encode(long uuid) {
        return Long.toHexString(uuid);
    }

    public static long decode(String uuid) {
        return UnsignedLongs.parseUnsignedLong(uuid, 16);
    }
}
