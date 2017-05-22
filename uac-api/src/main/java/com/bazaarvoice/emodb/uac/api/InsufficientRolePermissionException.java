package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Exception thrown when a caller attempts to grant permissions for a role and the caller does not have
 * permission to grant the permissions.  The is different from receiving an
 * {@link com.bazaarvoice.emodb.common.api.UnauthorizedException}.  An UnauthorizedException indicates the caller
 * doesn't have permission to grant <em>any</em> permissions to the role.  This exception indicates the caller generally
 * has permission to grant permissions for the role but not the specific permission being attempted.  The same
 * applies when revoking permissions from a role.
 *
 * For example, assume API Key <i>k1</i> has permission to grant any blob related permission to role <i>r1</i>, as
 * encompassed by the permission "blob|*".  If <i>k1</i> attempted to grant "sor|read|*" to <i>r1</i> it would
 * receive this exception.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class InsufficientRolePermissionException extends RuntimeException {

    private String _permission;

    public InsufficientRolePermissionException() {
        this("Insufficient permission to grant or revoke permission", null);
    }

    @JsonCreator
    public InsufficientRolePermissionException(@JsonProperty("message") String message, @JsonProperty("permission") String permission) {
        super(message);
        _permission = permission;
    }

    public String getPermission() {
        return _permission;
    }
}
