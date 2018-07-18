package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Request object for updating an existing role.  The request can update the role's name, description, permissions
 * associated with the role, , and any combination of these.  Only those values which the caller wants changed should be
 * explicitly set; all other values will remain unchanged.  The "roleKey" attribute is required.
 *
 * When updating permissions the caller does not need to provide the full set of permissions for the role.
 * Permissions can be incrementally granted and revoked, leaving all permissions not explicitly provided in place.
 * If desired, the request can be configured to revoke all existing permissions not explicitly granted by setting
 * {@link #setRevokeOtherPermissions(boolean)} to true; the default is false.
 */
@JsonSerialize(using = UpdateEmoRoleRequest.UpdateRoleRequestSerializer.class)
public class UpdateEmoRoleRequest extends UserAccessControlRequest {
    private EmoRoleKey _roleKey;
    private String _name;
    private boolean _namePresent;
    private String _description;
    private boolean _descriptionPresent;
    private Set<String> _grantedPermissions = new HashSet<>();
    private Set<String> _revokedPermissions = new HashSet<>();
    private boolean _revokeOtherPermissions;

    @JsonCreator
    public UpdateEmoRoleRequest() {
        // empty
    }

    public UpdateEmoRoleRequest(EmoRoleKey roleKey) {
        setRoleKey(roleKey);
    }

    public UpdateEmoRoleRequest(String group, String id) {
        this(new EmoRoleKey(group, id));
    }

    public UpdateEmoRoleRequest setRoleKey(EmoRoleKey roleKey) {
        _roleKey = requireNonNull(roleKey, "roleKey");
        return this;
    }

    @JsonIgnore
    public EmoRoleKey getRoleKey() {
        return _roleKey;
    }

    public String getName() {
        return _name;
    }

    public UpdateEmoRoleRequest setName(String name) {
        _name = name;
        _namePresent = true;
        return this;
    }

    public boolean isNamePresent() {
        return _namePresent;
    }

    public String getDescription() {
        return _description;
    }

    public UpdateEmoRoleRequest setDescription(String description) {
        _description = description;
        _descriptionPresent = true;
        return this;
    }

    public boolean isDescriptionPresent() {
        return _descriptionPresent;
    }

    public Set<String> getGrantedPermissions() {
        return Collections.unmodifiableSet(new HashSet<>(_grantedPermissions));
    }

    @JsonProperty("grantPermissions")
    public UpdateEmoRoleRequest setGrantedPermissions(Set<String> grantedPermissions) {
        _grantedPermissions.clear();
        return grantPermissions(grantedPermissions);
    }

    public UpdateEmoRoleRequest grantPermissions(Set<String> grantedPermissions) {
        if (grantedPermissions.stream().anyMatch(_revokedPermissions::contains)) {
            throw new IllegalArgumentException("Cannot both grant and revoke the same permission");
        }
        _grantedPermissions.addAll(grantedPermissions);
        return this;
    }

    public Set<String> getRevokedPermissions() {
        return Collections.unmodifiableSet(new HashSet<>(_revokedPermissions));
    }

    @JsonProperty("revokePermissions")
    public UpdateEmoRoleRequest setRevokedPermissions(Set<String> revokedPermissions) {
        _revokedPermissions.clear();
        return revokePermissions(revokedPermissions);
    }

    public UpdateEmoRoleRequest revokePermissions(Set<String> revokedPermissions) {
        if (revokedPermissions.stream().anyMatch(_grantedPermissions::contains)) {
            throw new IllegalArgumentException("Cannot both grant and revoke the same permission");
        }
        _revokedPermissions.addAll(revokedPermissions);
        return this;
    }

    public boolean isRevokeOtherPermissions() {
        return _revokeOtherPermissions;
    }

    public UpdateEmoRoleRequest setRevokeOtherPermissions(boolean revokeOtherPermissions) {
        _revokeOtherPermissions = revokeOtherPermissions;
        return this;
    }

    /**
     * Custom serializer to omit values which have not been explicitly set.  This allows the recipient to concisely
     * distinguish between explicitly setting a value to null and leaving a value unchanged.
     */
    static class UpdateRoleRequestSerializer extends JsonSerializer<UpdateEmoRoleRequest> {
        @Override
        public void serialize(UpdateEmoRoleRequest request, JsonGenerator gen, SerializerProvider provider)
                throws IOException, JsonProcessingException {
            gen.writeStartObject();
            if (request.isNamePresent()) {
                gen.writeStringField("name", request.getName());
            }
            if (request.isDescriptionPresent()) {
                gen.writeStringField("description", request.getDescription());
            }
            if (!request.getGrantedPermissions().isEmpty()) {
                gen.writeObjectField("grantPermissions", request.getGrantedPermissions());
            }
            if (!request.getRevokedPermissions().isEmpty()) {
                gen.writeObjectField("revokePermissions", request.getRevokedPermissions());
            }
            gen.writeBooleanField("revokeOtherPermissions", request.isRevokeOtherPermissions());
            gen.writeEndObject();
        }
    }
}
