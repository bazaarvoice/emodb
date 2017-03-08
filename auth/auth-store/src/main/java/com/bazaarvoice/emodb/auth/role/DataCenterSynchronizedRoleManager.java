package com.bazaarvoice.emodb.auth.role;

import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Role manager implementation which synchronizes mutative role updates in the current data center to prevent possible
 * inconsistencies caused by concurrent updates.  To do this it uses the ZooKeeper {@link InterProcessMutex} recipe.
 * Note that other protections are required to prevent global concurrent updates across all data centers.
 */
public class DataCenterSynchronizedRoleManager implements RoleManager {

    private final static String LOCK_PATH = "/lock/roles";

    private final RoleManager _delegate;
    private final InterProcessMutex _mutex;

    public DataCenterSynchronizedRoleManager(RoleManager delegate, CuratorFramework curator) {
        _delegate = checkNotNull(delegate, "delegate");
        _mutex = new InterProcessMutex(checkNotNull(curator, "curator"), LOCK_PATH);
    }

    @Override
    public Role getRole(RoleIdentifier id) {
        return _delegate.getRole(id);
    }

    @Override
    public List<Role> getRolesByGroup(@Nullable String group) {
        return _delegate.getRolesByGroup(group);
    }

    @Override
    public Iterator<Role> getAll() {
        return _delegate.getAll();
    }

    @Override
    public Set<String> getPermissionsForRole(RoleIdentifier id) {
        return _delegate.getPermissionsForRole(id);
    }

    @Override
    public Role createRole(RoleIdentifier id, RoleUpdateRequest request) {
        return inMutex(() -> _delegate.createRole(id, request));
    }

    @Override
    public void updateRole(RoleIdentifier id, RoleUpdateRequest request) {
        inMutex(() -> _delegate.updateRole(id, request));
    }

    @Override
    public void deleteRole(RoleIdentifier id) {
        inMutex(() -> _delegate.deleteRole(id));
    }

    private <T> T inMutex(Runnable runnable) {
        return inMutex(() -> { runnable.run(); return null; });
    }

    private <T> T inMutex(Callable<T> callable) {
        try {
            if (!_mutex.acquire(100, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException();
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
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
            throw Throwables.propagate(exception);
        }

        return result;
    }
}
