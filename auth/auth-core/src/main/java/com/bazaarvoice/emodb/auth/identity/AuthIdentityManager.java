package com.bazaarvoice.emodb.auth.identity;

import java.util.Set;

/**
 * Manager for identities.
 */
public interface AuthIdentityManager<T extends AuthIdentity> {

    /**
     * Gets an entity by ID.  Returns the identity, or null if no such identity exists.
     */
    T getIdentity(String id);

    /**
     * Updates an identity, or creates one if no matching identity already exists.
     */
    void updateIdentity(T identity);

    /**
     * Deletes an identity.
     */
    void deleteIdentity(String id);

    /**
     * Gets the roles associated with an identity by its internal ID.
     */
    Set<String> getRolesByInternalId(String internalId);
}
