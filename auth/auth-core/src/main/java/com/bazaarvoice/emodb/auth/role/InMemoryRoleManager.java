package com.bazaarvoice.emodb.auth.role;

import com.bazaarvoice.emodb.auth.permissions.PermissionIDs;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Simple in-memory implementation of a {@link RoleManager}.
 */
public class InMemoryRoleManager implements RoleManager {

    private final Map<RoleIdentifier, Role> _rolesById = Maps.newConcurrentMap();
    private final PermissionManager _permissionManager;

    public InMemoryRoleManager(PermissionManager permissionManager) {
        _permissionManager = permissionManager;
    }

    @Override
    public Role getRole(RoleIdentifier id) {
        return _rolesById.get(id);
    }

    @Override
    public List<Role> getRolesByGroup(@Nullable String group) {
        return _rolesById.values().stream()
                .filter(role -> Objects.equals(role.getGroup(), group))
                .collect(Collectors.toList());
    }

    @Override
    public Iterator<Role> getAll() {
        return _rolesById.values().iterator();
    }

    @Override
    public Set<String> getPermissionsForRole(RoleIdentifier id) {
        return ImmutableSortedSet.copyOf(
                _permissionManager.getPermissions(PermissionIDs.forRole(id))
                        .stream()
                        .map(Objects::toString)
                        .iterator());
    }

    @Override
    public Role createRole(RoleIdentifier id, RoleUpdateRequest request) {
        if (getRole(id) != null) {
            throw new RoleExistsException(id.getGroup(), id.getId());
        }
        Role role = new Role(id.getGroup(), id.getId(), request.getName(), request.getDescription());
        _rolesById.put(id, role);
        if (request.getPermissionUpdate() != null) {
            List<String> permissions = ImmutableList.copyOf(request.getPermissionUpdate().getPermitted());
            if (!permissions.isEmpty()) {
                _permissionManager.updatePermissions(PermissionIDs.forRole(id), new PermissionUpdateRequest().permit(permissions));
            }
        }
        return role;
    }

    @Override
    public void updateRole(RoleIdentifier id, RoleUpdateRequest request) {
        Role role = getRole(id);
        if (role == null) {
            throw new RoleNotFoundException(id.getGroup(), id.getId());
        }
        if (request.isDescriptionPresent()) {
            role.setDescription(request.getDescription());
        }
        if (request.getPermissionUpdate() != null) {
            _permissionManager.updatePermissions(PermissionIDs.forRole(id), request.getPermissionUpdate());
        }
    }

    @Override
    public void deleteRole(RoleIdentifier id) {
        _permissionManager.revokePermissions(PermissionIDs.forRole(id));
        _rolesById.remove(id);
    }
}
