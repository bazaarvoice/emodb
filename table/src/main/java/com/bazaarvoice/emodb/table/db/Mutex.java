package com.bazaarvoice.emodb.table.db;

import org.joda.time.Duration;

public interface Mutex {
    void runWithLock(Runnable runnable, Duration acquireTimeout);
}
