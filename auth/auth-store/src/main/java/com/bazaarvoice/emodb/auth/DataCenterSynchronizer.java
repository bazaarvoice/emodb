package com.bazaarvoice.emodb.auth;

import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * Utility class for serially executing operations within the local data center using a the ZooKeeper
 * {@link InterProcessMutex} recipe.  Note that other protections are required to prevent global concurrent updates
 * across all data centers.
 */
public class DataCenterSynchronizer {

    private final InterProcessMutex _mutex;

    public DataCenterSynchronizer(CuratorFramework curator, String lockPath) {
        _mutex = new InterProcessMutex(requireNonNull(curator, "curator"), requireNonNull(lockPath, "lockPath"));
    }

    public void inMutex(Runnable runnable) {
        inMutex(() -> { runnable.run(); return null; });
    }

    public <T> T inMutex(Callable<T> callable) {
        try {
            if (!_mutex.acquire(100, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException();
            }
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }

        Exception exception = null;
        T result = null;
        try {
            result = callable.call();
        } catch (Exception e) {
            exception = e;
        } finally {
            try {
                _mutex.release();
            } catch (Exception e) {
                // If the callable raised an exception too prefer raising that one.
                if (exception == null) {
                    exception = e;
                }
            }
        }

        if (exception != null) {
            Throwables.throwIfUnchecked(exception);
            throw new RuntimeException(exception);
        }

        return result;
    }
}
