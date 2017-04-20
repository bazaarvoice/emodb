package com.bazaarvoice.emodb.auth.identity;

/**
 * Manager for identities.
 */
public interface AuthIdentityManager<T extends AuthIdentity> extends AuthIdentityReader<T> {

    /**
     * Creates an identity.
     * @return The unique ID for the new identity
     * @throws IdentityExistsException if either the provided ID or authentication ID are already in use.
     */
    String createIdentity(String authenticationId, AuthIdentityModification<T> modification)
            throws IdentityExistsException;
    
    /**
     * Updates an identity.
     */
    void updateIdentity(String id, AuthIdentityModification<T> modification)
            throws IdentityNotFoundException;

    /**
     * Migrates an identity to a new authentication ID.
     * @throws IdentityNotFoundException if no identity matching the ID exists
     * @throws IdentityExistsException if another identity matching the authentication ID exists
     */
    void migrateIdentity(String id, String newAuthenticationId)
            throws IdentityNotFoundException, IdentityExistsException;

    /**
     * Deletes an identity.
     */
    void deleteIdentity(String id);
}
