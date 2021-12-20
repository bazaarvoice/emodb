package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import java.util.Date;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * API key object for an Emo user.  Each API key has two identifiers:  The private key used for authentication and
 * authorization and an ID.  Because the private key is private it is not contained within this object.  There is
 * a {@link #getMaskedKey()} method which returns a version of the key with a significant number of the key's
 * characters hidden.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EmoApiKey {

    private final String _id;
    private final String _maskedKey;
    private final Date _issued;
    private String _owner;
    private String _description;
    private Set<EmoRoleKey> _roles = ImmutableSet.of();

    @JsonCreator
    public EmoApiKey(@JsonProperty("id") String id, @JsonProperty("maskedKey") String maskedKey,
                     @JsonProperty("issued") Date issued) {
        _id = requireNonNull(id, "id");
        _maskedKey = requireNonNull(maskedKey, "maskedKey");
        _issued = requireNonNull(issued, "issued");
    }

    public String getId() {
        return _id;
    }

    public String getMaskedKey() {
        return _maskedKey;
    }

    public Date getIssued() {
        return _issued;
    }

    public String getOwner() {
        return _owner;
    }

    public EmoApiKey setOwner(String owner) {
        _owner = owner;
        return this;
    }

    public String getDescription() {
        return _description;
    }

    public EmoApiKey setDescription(String description) {
        _description = description;
        return this;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Set<EmoRoleKey> getRoles() {
        return _roles;
    }

    public EmoApiKey setRoles(Set<EmoRoleKey> roles) {
        _roles = requireNonNull(roles, "roles");
        return this;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(EmoApiKey.class)
                .add("id", getId())
                .add("owner", getOwner())
                .add("description", getDescription())
                .add("roles", getRoles())
                .toString();
    }
}
