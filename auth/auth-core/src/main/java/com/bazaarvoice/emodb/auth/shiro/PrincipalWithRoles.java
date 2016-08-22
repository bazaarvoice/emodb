package com.bazaarvoice.emodb.auth.shiro;

import com.google.common.collect.ImmutableSet;
import org.eclipse.jetty.security.DefaultUserIdentity;
import org.eclipse.jetty.server.UserIdentity;

import java.security.Principal;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class PrincipalWithRoles implements Principal {
    private final String _id;
    private final Set<String> _roles;
    private UserIdentity _userIdentity;

    public PrincipalWithRoles(String id, Set<String> roles) {
        _id = checkNotNull(id, "id");
        _roles = checkNotNull(roles, "roles");
    }

    @Override
    public String getName() {
        return _id;
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
}
