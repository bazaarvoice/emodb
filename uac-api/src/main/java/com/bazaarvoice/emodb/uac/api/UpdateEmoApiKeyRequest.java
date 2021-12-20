package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Request object for updating an existing API key.  The request can update the key's owner, description, roles
 * associated with the key, and any combination of these.  Only those values which the caller wants changed should be
 * explicitly set; all other values will remain unchanged.  The "id" attribute is required and, if "owner" is updated,
 * it cannot be null.
 *
 * When updating roles the caller does not need to provide the full set of roles for the API key.  Roles can be
 * be incrementally assigned and unassigned, leaving all roles not explicitly provided in place.  If desired, the
 * request can be configured to unassign all existing roles not explicitly assigned by setting
 * {@link #setUnassignOtherRoles(boolean)} to true; the default is false.
 */
@JsonSerialize(using = UpdateEmoApiKeyRequest.UpdateApiKeyRequestSerializer.class)
public class UpdateEmoApiKeyRequest extends UserAccessControlRequest {

    private String _id;
    private String _owner;
    private boolean _ownerPresent;
    private String _description;
    private boolean _descriptionPresent;
    private Set<EmoRoleKey> _assignedRoles = Sets.newHashSet();
    private Set<EmoRoleKey> _unassignedRoles = Sets.newHashSet();
    private boolean _unassignOtherRoles;

    @JsonCreator
    public UpdateEmoApiKeyRequest() {
        // empty
    }

    public UpdateEmoApiKeyRequest(String id) {
        setId(id);
    }

    public UpdateEmoApiKeyRequest setId(String id) {
        checkArgument(!Strings.isNullOrEmpty(id), "id");
        _id = id;
        return this;
    }

    @JsonIgnore
    public String getId() {
        return _id;
    }

    public String getOwner() {
        return _owner;
    }

    public UpdateEmoApiKeyRequest setOwner(String owner) {
        _owner = owner;
        _ownerPresent = true;
        return this;
    }

    public boolean isOwnerPresent() {
        return _ownerPresent;
    }

    public String getDescription() {
        return _description;
    }

    public UpdateEmoApiKeyRequest setDescription(String description) {
        _description = description;
        _descriptionPresent = true;
        return this;
    }

    public boolean isDescriptionPresent() {
        return _descriptionPresent;
    }

    public Set<EmoRoleKey> getAssignedRoles() {
        return ImmutableSet.copyOf(_assignedRoles);
    }

    @JsonProperty("assignRoles")
    public UpdateEmoApiKeyRequest setAssignedRoles(Set<EmoRoleKey> assignedRoles) {
        _assignedRoles.clear();
        return assignRoles(assignedRoles);
    }

    public UpdateEmoApiKeyRequest assignRoles(Set<EmoRoleKey> addedRoles) {
        requireNonNull(addedRoles, "roles");
        checkArgument(Sets.intersection(addedRoles, _unassignedRoles).isEmpty(),
                "Cannot both assign and unassign the same role");
        _assignedRoles.addAll(addedRoles);
        return this;
    }

    public Set<EmoRoleKey> getUnassignedRoles() {
        return ImmutableSet.copyOf(_unassignedRoles);
    }

    @JsonProperty("unassignRoles")
    public UpdateEmoApiKeyRequest setUnassignedRoles(Set<EmoRoleKey> unassignedRoles) {
        _unassignedRoles.clear();
        return unassignRoles(unassignedRoles);
    }

    public UpdateEmoApiKeyRequest unassignRoles(Set<EmoRoleKey> unassignedRoles) {
        requireNonNull(unassignedRoles, "roles");
        checkArgument(Sets.intersection(unassignedRoles, _assignedRoles).isEmpty(),
                "Cannot both assign and unassign the same role");
        _unassignedRoles.addAll(unassignedRoles);
        return this;
    }

    public boolean isUnassignOtherRoles() {
        return _unassignOtherRoles;
    }

    public UpdateEmoApiKeyRequest setUnassignOtherRoles(boolean unassignOtherRoles) {
        _unassignOtherRoles = unassignOtherRoles;
        return this;
    }

    /**
     * Custom serializer to omit values which have not been explicitly set.  This allows the recipient to concisely
     * distinguish between explicitly setting a value to null and leaving a value unchanged.
     */
    static class UpdateApiKeyRequestSerializer extends JsonSerializer<UpdateEmoApiKeyRequest> {
        @Override
        public void serialize(UpdateEmoApiKeyRequest request, JsonGenerator gen, SerializerProvider provider)
                throws IOException, JsonProcessingException {
            gen.writeStartObject();
            if (request.isOwnerPresent()) {
                gen.writeStringField("owner", request.getOwner());
            }
            if (request.isDescriptionPresent()) {
                gen.writeStringField("description", request.getDescription());
            }
            if (!request.getAssignedRoles().isEmpty()) {
                gen.writeObjectField("assignRoles", request.getAssignedRoles());
            }
            if (!request.getUnassignedRoles().isEmpty()) {
                gen.writeObjectField("unassignRoles", request.getUnassignedRoles());
            }
            gen.writeBooleanField("unassignOtherRoles", request.isUnassignOtherRoles());
            gen.writeEndObject();
        }
    }
}
