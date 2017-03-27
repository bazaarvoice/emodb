package com.bazaarvoice.emodb.auth.identity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import java.util.Date;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base unit of identity for a client which can be authenticated in the application.
 */
abstract public class AuthIdentity {

    /**
     * Each identity is associated with an immutable ID distinct from the secret authentication key.  This is done
     * for several reasons:
     *
     * <ol>
     *     <li>
     *         Parts of the system may need associate resources with an ID.  For example, each databus subscription
     *         is associated with an API key.  By using a distinct  ID these parts of the system don't need to
     *         be concerned with safely storing the API key, since the ID is essentially a reference to the key.
     *     </li>
     *     <li>
     *         Parts of the system may need to determine whether an ID has permission to perform certain actions
     *         without actually logging the user in.  Databus fanout is a prime example of this.  Using IDs
     *         in the interface allows those parts of the system to validate authorization for permission without
     *         the ability to impersonate that user by logging them in.
     *     </li>
     *     <li>
     *         If an ID is compromised an administrator may want to replace it with a new ID without
     *         changing all internal references to that ID.
     *     </li>
     *     <li>
     *         User management can be performed without the identity owner divulging her authentication ID,
     *         such as over a chat client or in an email.
     *     </li>
     * </ol>
     */
    private final String _id;

    // Roles assigned to the identity
    private final Set<String> _roles;
    // Owner of the identity, such as an email address
    private String _owner;
    // Description of this identity
    private String _description;
    // Date this identity was issued
    private Date _issued;
    // Masked version of the authentication ID.
    private String _maskedId;

    protected AuthIdentity(String id, Set<String> roles) {
        checkArgument(!Strings.isNullOrEmpty(id), "id");
        _id = id;
        _roles = ImmutableSet.copyOf(checkNotNull(roles, "roles"));
    }

    public String getId() {
        return _id;
    }

    public Set<String> getRoles() {
        return _roles;
    }

    public String getOwner() {
        return _owner;
    }

    public void setOwner(String owner) {
        _owner = owner;
    }

    public String getDescription() {
        return _description;
    }

    public void setDescription(String description) {
        _description = description;
    }

    public Date getIssued() {
        return _issued;
    }

    public void setIssued(Date issued) {
        _issued = issued;
    }

    public String getMaskedId() {
        return _maskedId;
    }

    public void setMaskedId(String maskedId) {
        _maskedId = maskedId;
    }
}
