package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Request object for creating new roles.  Parameters include the key for the new role (group and ID),
 * attributes such as name and description, and the set of initial permissions for this role.  With the exception
 * of "roleKey" all other parameters are optional, although it is recommended to provide at least a user-friendly name
 * for each role.
 */
public class CreateEmoRoleRequest extends UserAccessControlRequest {
    private EmoRoleKey _roleKey;
    private String _name;
    private String _description;
    private Set<String> _permissions = ImmutableSet.of();

    @JsonCreator
    public CreateEmoRoleRequest() {
        // empty
    }

    public CreateEmoRoleRequest(EmoRoleKey roleKey) {
        setRoleKey(roleKey);
    }

    public CreateEmoRoleRequest(String group, String id) {
        this(new EmoRoleKey(group, id));
    }

    public CreateEmoRoleRequest setRoleKey(EmoRoleKey roleKey) {
        _roleKey = checkNotNull(roleKey, "roleKey");
        return this;
    }

    @JsonIgnore
    public EmoRoleKey getRoleKey() {
        return _roleKey;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getName() {
        return _name;
    }

    public CreateEmoRoleRequest setName(String name) {
        _name = name;
        return this;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getDescription() {
        return _description;
    }

    public CreateEmoRoleRequest setDescription(String description) {
        _description = description;
        return this;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Set<String> getPermissions() {
        return _permissions;
    }

    public CreateEmoRoleRequest setPermissions(Set<String> permissions) {
        _permissions = MoreObjects.firstNonNull(permissions, ImmutableSet.<String>of());
        return this;
    }
}
