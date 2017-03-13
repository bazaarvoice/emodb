package com.bazaarvoice.emodb.auth.role;

import com.bazaarvoice.emodb.auth.DataCenterSynchronizer;
import org.apache.curator.framework.CuratorFramework;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Role manager implementation which synchronizes mutative role updates in the current data center to prevent possible
 * inconsistencies caused by concurrent updates.  Note that other protections are required to prevent global concurrent
 * updates across all data centers.
 */
public class DataCenterSynchronizedRoleManager implements RoleManager {

    private final static String LOCK_PATH = "/lock/roles";

    private final RoleManager _delegate;
    private final DataCenterSynchronizer _synchronizer;

    public DataCenterSynchronizedRoleManager(RoleManager delegate, CuratorFramework curator) {
        _delegate = checkNotNull(delegate, "delegate");
        _synchronizer = new DataCenterSynchronizer(curator, LOCK_PATH);
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
    public Role createRole(RoleIdentifier id, RoleModification modification) {
        return _synchronizer.inMutex(() -> _delegate.createRole(id, modification));
    }

    @Override
    public void updateRole(RoleIdentifier id, RoleModification modification) {
        _synchronizer.inMutex(() -> _delegate.updateRole(id, modification));
    }

    @Override
    public void deleteRole(RoleIdentifier id) {
        _synchronizer.inMutex(() -> _delegate.deleteRole(id));
    }
}
