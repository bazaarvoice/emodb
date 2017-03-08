package com.bazaarvoice.emodb.auth.identity;

import org.apache.shiro.authz.AuthorizationInfo;

import java.util.Set;

/**
 * Minimal interface for read-only access to authentication identities.
 */
public interface AuthIdentityReader<T extends AuthIdentity> {

    /**
     * Gets an entity by ID, such as its API key.  Returns the identity, or null if no such identity exists.
     */
    T getIdentity(String id);

    /**
     * Gets the roles associated with an identity by its internal ID.
     *
     * Although role management is done using {@link com.bazaarvoice.emodb.auth.role.RoleIdentifier} Shiro's
     * authorization framework represents roles as Strings, as demonstrated by {@link AuthorizationInfo#getRoles())}.
     * Since this reader is used as part of the authorization framework it provides an interface compatible with what
     * Shiro requires.
     */
    Set<String> getRolesByInternalId(String internalId);
}
