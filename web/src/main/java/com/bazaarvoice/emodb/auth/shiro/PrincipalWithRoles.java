package com.bazaarvoice.emodb.auth.shiro;

import com.google.common.collect.ImmutableSet;
import org.apache.http.auth.BasicUserPrincipal;
import org.eclipse.jetty.security.DefaultUserIdentity;
import org.eclipse.jetty.server.UserIdentity;

import java.security.Principal;
import java.util.Set;

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

public class PrincipalWithRoles implements Principal {
    private final String _authenticationId;
    private final String _id;
    private final Set<String> _roles;
    private UserIdentity _userIdentity;

    public PrincipalWithRoles(String authenticationId, String id, Set<String> roles) {
        _authenticationId = requireNonNull(authenticationId, "authenticationId");
        _id = requireNonNull(id, "id");
        _roles = requireNonNull(roles, "roles");
    }

    @Override
    public String getName() {
        return _authenticationId;
    }

    public String getId() {
        return _id;
    }

    public Set<String> getRoles() {
        return _roles;
    }

    @Override
    public String toString() {
        return _authenticationId;
    }

    /**
     * Returns this instance as a Jetty UserIdentity.  The returned instance is immutable and cached.
     */
    public UserIdentity toUserIdentity() {
        if (_userIdentity == null) {
            String[] roles = _roles.toArray(new String[_roles.size()]);

            _userIdentity = new DefaultUserIdentity(
                    new javax.security.auth.Subject(true, ImmutableSet.of(this), ImmutableSet.of(), ImmutableSet.of()),
                    new BasicUserPrincipal(getId()), roles);
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

        return _authenticationId.equals(that._authenticationId) &&
                _id.equals(that._id) &&
                _roles.equals(that._roles);
    }

    @Override
    public int hashCode() {
        return hash(_authenticationId, _id);
    }
}
