package com.bazaarvoice.emodb.auth.role;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * RoleManager implementation which allows the caller to provide a static, immutable set of roles and a delegate
 * RoleManager.  This provides the ability to have a set of reserved roles which are not persisted by the delegate.
 */
public class DeferringRoleManager implements RoleManager {

    private final RoleManager _delegate;
    private final Map<RoleIdentifier, Role> _rolesById;

    public DeferringRoleManager(RoleManager delegate, Collection<Role> roles) {
        _delegate = delegate;
        _rolesById = Maps.uniqueIndex(roles, role -> new RoleIdentifier(role.getGroup(), role.getName()));
    }

    @Override
    public Role getRole(RoleIdentifier id) {
        checkNotNull(id, "id");
        Role role = _rolesById.get(id);
        if (role == null) {
            role = _delegate.getRole(id);
        }
        return role;
    }

    @Override
    public List<Role> getRolesByGroup(@Nullable String group) {
        final List<Role> roles = Lists.newArrayList();
        _rolesById.values().stream()
                .filter(role -> Objects.equals(role.getGroup(), group))
                .forEach(roles::add);
        roles.addAll(_delegate.getRolesByGroup(group));
        return roles;
    }

    @Override
    public Iterator<Role> getAll() {
        return Iterators.concat(_rolesById.values().iterator(), _delegate.getAll());
    }

    /**
     * Although this implementation overrides specific roles the permissions associated with them are
     * still managed by the permission manager, so always defer to the delegate to read permissions.
     */
    @Override
    public Set<String> getPermissionsForRole(RoleIdentifier id) {
        checkNotNull(id, "id");
        return _delegate.getPermissionsForRole(id);
    }

    @Override
    public Role createRole(RoleIdentifier id, RoleUpdateRequest request) {
        checkNotNull(id, "id");
        if (_rolesById.containsKey(id)) {
            throw new RoleExistsException(id.getGroup(), id.getId());
        }
        return _delegate.createRole(id, request);
    }

    @Override
    public void updateRole(RoleIdentifier id, RoleUpdateRequest request) {
        checkNotNull(id, "id");
        checkArgument(!_rolesById.containsKey(id), "Cannot update role %s", id);
        _delegate.updateRole(id, request);
    }

    @Override
    public void deleteRole(RoleIdentifier id) {
        checkNotNull(id, "id");
        checkArgument(!_rolesById.containsKey(id), "Cannot delete role %s", id);
        _delegate.deleteRole(id);
    }
}
