package com.bazaarvoice.emodb.table.db;

import java.time.Duration;

public interface Mutex {
    void runWithLock(Runnable runnable, Duration acquireTimeout);
}
