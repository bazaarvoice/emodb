package com.bazaarvoice.emodb.auth.role;

import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;

/**
 * Request parameter for updating a role using {@link RoleManager#updateRole(RoleIdentifier, RoleUpdateRequest)}.
 */
public class RoleUpdateRequest {

    private String _description;
    private boolean _descriptionPresent = false;
    private PermissionUpdateRequest _permissionUpdate;

    public RoleUpdateRequest withDescription(String description) {
        _description = description;
        _descriptionPresent = true;
        return this;
    }

    public RoleUpdateRequest withPermissionUpdate(PermissionUpdateRequest permissionUpdate) {
        _permissionUpdate = permissionUpdate;
        return this;
    }

    public String getDescription() {
        return _description;
    }

    public boolean isDescriptionPresent() {
        return _descriptionPresent;
    }

    public PermissionUpdateRequest getPermissionUpdate() {
        return _permissionUpdate;
    }
}
