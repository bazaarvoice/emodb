package com.bazaarvoice.emodb.table.db.curator;

import com.bazaarvoice.emodb.table.db.Mutex;
import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.joda.time.Duration;

public class TableMutexManager {

    private static final int NUM_MUTEXES = 256;

    private final Mutex[] _mutexes;
    private final CuratorMutex _oldMutex;

    public TableMutexManager(CuratorFramework curator, String oldPath, String newPath) {
        _mutexes = new Mutex[NUM_MUTEXES];
        for (int i = 0; i < NUM_MUTEXES; i++) {
            _mutexes[i] = new CuratorMutex(curator, ZKPaths.makePath(newPath, Integer.toString(i)));
        }
        _oldMutex = new CuratorMutex(curator, oldPath);
    }

    public void runWithLockForTable(Runnable runnable, Duration acquireTimeout, String table) {


        _oldMutex.runWithLock(() -> {
            _mutexes[Math.abs(Hashing.murmur3_128().hashString(table, Charsets.UTF_8).asInt() % NUM_MUTEXES)]
                            .runWithLock(runnable, acquireTimeout);
        }, acquireTimeout);

    }
}
