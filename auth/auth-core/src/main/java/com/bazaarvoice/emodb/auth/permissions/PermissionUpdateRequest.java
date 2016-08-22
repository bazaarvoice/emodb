package com.bazaarvoice.emodb.auth.permissions;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Helper class for updating permissions using {@link PermissionManager#updateForRole(String, PermissionUpdateRequest)}
 */
public class PermissionUpdateRequest {

    private final Map<String, Boolean> _permissions = Maps.newLinkedHashMap();

    public PermissionUpdateRequest permit(Iterable<String> permissions) {
        update(Boolean.TRUE, permissions);
        return this;
    }

    public PermissionUpdateRequest permit(String... permissions) {
        return permit(Arrays.asList(permissions));
    }

    public PermissionUpdateRequest revoke(Iterable<String> permissions) {
        update(Boolean.FALSE, permissions);
        return this;
    }

    public PermissionUpdateRequest revoke(String... permissions) {
        return revoke(Arrays.asList(permissions));
    }

    private void update(Boolean permitted, Iterable<String> permissions) {
        for (String permission : permissions) {
            checkNotNull(permission, "permission");
            _permissions.put(permission, permitted);
        }
    }

    public Iterable<String> getPermitted() {
        return Maps.filterValues(_permissions, Predicates.equalTo(Boolean.TRUE)).keySet();
    }

    public Iterable<String> getRevoked() {
        return Maps.filterValues(_permissions, Predicates.equalTo(Boolean.FALSE)).keySet();
    }
}
