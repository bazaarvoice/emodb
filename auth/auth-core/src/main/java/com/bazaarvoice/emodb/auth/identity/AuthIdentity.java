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
abstract public class AuthIdentity {

    // ID of the identity
    private final String _id;
    // Roles assigned to the identity
    private final Set<String> _roles;
    // Owner of the identity, such as an email address
    private String _owner;
    // Description of this identity
    private String _description;
    // Date this identity was issued
    private Date _issued;


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
}
