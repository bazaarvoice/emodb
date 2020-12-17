package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

/**
 * Request object for creating new API keys.  Parameters include attributes such as owner and description and the set of
 * initial roles assigned to the new key.  "Owner" is the only required attribute.
 */
public class CreateEmoApiKeyRequest extends UserAccessControlRequest {

    private String _owner;
    private String _description;
    private Set<EmoRoleKey> _roles = ImmutableSet.of();

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getOwner() {
        return _owner;
    }

    public CreateEmoApiKeyRequest setOwner(String owner) {
        _owner = owner;
        return this;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getDescription() {
        return _description;
    }

    public CreateEmoApiKeyRequest setDescription(String description) {
        _description = description;
        return this;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Set<EmoRoleKey> getRoles() {
        return _roles;
    }

    public CreateEmoApiKeyRequest setRoles(Set<EmoRoleKey> roles) {
        _roles = Optional.ofNullable(roles).orElse(ImmutableSet.of());
        return this;
    }
}
