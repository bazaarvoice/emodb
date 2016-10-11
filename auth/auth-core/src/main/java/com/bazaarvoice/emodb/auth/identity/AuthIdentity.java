package com.bazaarvoice.emodb.auth.identity;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import java.util.Date;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base unit of identity for a client which can be authenticated in the application.
 */
abstract public class AuthIdentity implements InternalIdentity {

    /**
     * Each identity is associated with an internal ID which is never exposed outside the system.  This is done
     * for several reasons:
     *
     * <ol>
     *     <li>
     *         Parts of the system may need associate resources with an ID.  For example, each databus subscription
     *         is associated with an API key.  By using an internal ID these parts of the system don't need to
     *         be concerned with safely storing the API key, since the internal ID is essentially a reference to
     *         the key.
     *     </li>
     *     <li>
     *         Parts of the system may need to determine whether an ID has permission to perform certain actions
     *         without actually logging the user in.  Databus fanout is a prime example of this.  Using internal IDs
     *         in the interface allows those parts of the system to validate authorization for permission without
     *         the ability to impersonate that user by logging them in.
     *     </li>
     *     <li>
     *         If an ID is compromised an administrator may want to replace it with a new ID without
     *         changing all internal references to that ID.
     *     </li>
     * </ol>
     */
    private final String _internalId;

    // Client-facing ID of the identity
    private final String _id;
    // Roles assigned to the identity
    private final Set<String> _roles;
    // Owner of the identity, such as an email address
    private String _owner;
    // Description of this identity
    private String _description;
    // Date this identity was issued
    private Date _issued;
    // State for this identity
    private IdentityState _state;

    protected AuthIdentity(String id, String internalId, IdentityState state, Set<String> roles) {
        checkArgument(!Strings.isNullOrEmpty(id), "id");
        checkArgument(!Strings.isNullOrEmpty(internalId), "internalId");
        _id = id;
        _internalId = internalId;
        _state = state;
        _roles = ImmutableSet.copyOf(checkNotNull(roles, "roles"));
    }

    public String getId() {
        return _id;
    }

    @Override
    public String getInternalId() {
        return _internalId;
    }

    @Override
    public Set<String> getRoles() {
        return _roles;
    }

    @Override
    public String getOwner() {
        return _owner;
    }

    public void setOwner(String owner) {
        _owner = owner;
    }

    @Override
    public String getDescription() {
        return _description;
    }

    public void setDescription(String description) {
        _description = description;
    }

    @Override
    public Date getIssued() {
        return _issued;
    }

    public void setIssued(Date issued) {
        _issued = issued;
    }

    @Override
    public IdentityState getState() {
        return _state;
    }

    public void setState(IdentityState state) {
        _state = state;
    }
}
