package com.bazaarvoice.emodb.auth.identity;

/**
 * Manager for identities.
 */
public interface AuthIdentityManager<T extends AuthIdentity> extends AuthIdentityReader<T> {

    /**
     * Updates an identity, or creates one if no matching identity already exists.
     */
    void updateIdentity(T identity);

    /**
     * Deletes an identity.
     */
    void deleteIdentity(String id);
}
