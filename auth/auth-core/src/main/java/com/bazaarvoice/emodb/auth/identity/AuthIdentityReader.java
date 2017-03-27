package com.bazaarvoice.emodb.auth.identity;

/**
 * Minimal interface for read-only access to authentication identities.
 */
public interface AuthIdentityReader<T extends AuthIdentity> {

    /**
     * Gets an identity by authentication ID, such as its API key.  Returns the identity, or null if no such identity exists.
     */
    T getIdentityByAuthenticationId(String authenticationId);

    /**
     * Gets an entity by its ID, or null if no such identity exists.
     */
    T getIdentity(String id);
}
