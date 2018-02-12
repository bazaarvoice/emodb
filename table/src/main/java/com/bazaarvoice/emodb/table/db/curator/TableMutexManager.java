package com.bazaarvoice.emodb.table.db.curator;

import com.bazaarvoice.emodb.table.db.Mutex;
import org.apache.curator.framework.CuratorFramework;
import org.joda.time.Duration;

public class TableMutexManager {

    private static final int NUM_MUTEXES = 256;

    private final Mutex[] _mutexes;

    public TableMutexManager(CuratorFramework curator, String path) {
        _mutexes = new Mutex[NUM_MUTEXES];
        for (int i = 0; i < NUM_MUTEXES; i++) {
            _mutexes[i] = new CuratorMutex(curator, path + "/" + i);
        }
    }

    public void runWithLockForTable(Runnable runnable, Duration acquireTimeout, String table) {
        _mutexes[table.hashCode() % NUM_MUTEXES].runWithLock(runnable, acquireTimeout);
    }
}
