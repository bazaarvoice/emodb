package com.bazaarvoice.emodb.sor.uuid;

import java.util.UUID;

/**
 * Operations on time-based UUIDs.
 */
public abstract class TimeUUIDs {

    /** Prevent instantiation. */
    private TimeUUIDs() {}

    public static UUID newUUID() {
        return com.bazaarvoice.emodb.common.uuid.TimeUUIDs.newUUID();
    }

    public static UUID getNext(UUID uuid) {
        return com.bazaarvoice.emodb.common.uuid.TimeUUIDs.getNext(uuid);
    }

    public static long getTimeMillis(UUID uuid) {
        return com.bazaarvoice.emodb.common.uuid.TimeUUIDs.getTimeMillis(uuid);
    }
}
