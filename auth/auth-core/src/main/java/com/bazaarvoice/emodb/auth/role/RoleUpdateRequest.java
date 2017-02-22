package com.bazaarvoice.emodb.auth.role;

import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;

/**
 * Request parameter for updating a role using {@link RoleManager#createRole(RoleIdentifier, RoleUpdateRequest)}
 * or {@link RoleManager#updateRole(RoleIdentifier, RoleUpdateRequest)}.
 */
public class RoleUpdateRequest {

    private String _name;
    private boolean _namePresent = false;
    private String _description;
    private boolean _descriptionPresent = false;
    private PermissionUpdateRequest _permissionUpdate;

    public RoleUpdateRequest withName(String name) {
        _name = name;
        _namePresent = true;
        return this;
    }

    public RoleUpdateRequest withDescription(String description) {
        _description = description;
        _descriptionPresent = true;
        return this;
    }

    public RoleUpdateRequest withPermissionUpdate(PermissionUpdateRequest permissionUpdate) {
        _permissionUpdate = permissionUpdate;
        return this;
    }

    public String getName() {
        return _name;
    }

    public boolean isNamePresent() {
        return _namePresent;
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
