package com.bazaarvoice.emodb.table.db.curator;

import com.bazaarvoice.emodb.table.db.Mutex;
import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * Data-center-wide mutex spans all instances of the DataStore for a particular deployment in a particular data center.
 */
public class CuratorMutex implements Mutex {
    private final CuratorFramework _curatorFramework;
    private final String _path;

    public CuratorMutex(CuratorFramework curatorFramework, String path) {
        _curatorFramework = requireNonNull(curatorFramework, "curatorFramework");
        _path = requireNonNull(path, "path");
    }

    @Override
    public void runWithLock(Runnable runnable, Duration acquireTimeout) {
        InterProcessMutex mutex = acquire(acquireTimeout);
        try {
            runnable.run();
        } finally {
            release(mutex);
        }
    }

    private InterProcessMutex acquire(Duration acquireTimeout) {
        InterProcessMutex mutex = new InterProcessMutex(_curatorFramework, _path);
        try {
            if (!mutex.acquire(acquireTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new TimeoutException();
            }
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        return mutex;
    }
    
    private void release(InterProcessMutex mutex) {
        try {
            mutex.release();
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
