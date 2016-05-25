package com.bazaarvoice.emodb.auth.shiro;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import org.eclipse.jetty.security.DefaultUserIdentity;
import org.eclipse.jetty.server.UserIdentity;

import java.security.Principal;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class PrincipalWithRoles implements Principal {
    private final String _id;
    private final String _internalId;
    private final Set<String> _roles;
    private UserIdentity _userIdentity;

    public PrincipalWithRoles(String id, String internalId, Set<String> roles) {
        _id = checkNotNull(id, "id");
        _internalId = checkNotNull(internalId, "internalId");
        _roles = checkNotNull(roles, "roles");
    }

    @Override
    public String getName() {
        return _id;
    }

    public String getInternalId() {
        return _internalId;
    }

    public Set<String> getRoles() {
        return _roles;
    }

    @Override
    public String toString() {
        return _id;
    }

    /**
     * Returns this instance as a Jetty UserIdentity.  The returned instance is immutable and cached.
     */
    public UserIdentity toUserIdentity() {
        if (_userIdentity == null) {
            String[] roles = _roles.toArray(new String[_roles.size()]);

            _userIdentity = new DefaultUserIdentity(
                    new javax.security.auth.Subject(true, ImmutableSet.of(this), ImmutableSet.of(), ImmutableSet.of()),
                    this, roles);
        }
        return _userIdentity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PrincipalWithRoles)) {
            return false;
        }

        PrincipalWithRoles that = (PrincipalWithRoles) o;

        return _id.equals(that._id) &&
                _internalId.equals(that._internalId) &&
                _roles.equals(that._roles);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_id, _internalId);
    }
}
