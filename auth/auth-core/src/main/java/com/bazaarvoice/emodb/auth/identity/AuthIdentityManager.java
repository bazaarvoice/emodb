package com.bazaarvoice.emodb.auth.identity;

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
     * Migrates an existing identity to a new identity.  The internal ID for the identity remains unchanged.
     * This is useful if an identity has been compromised and the secret ID needs to be changed while keeping all other
     * attributes of the identity unmodified.
     */
    void migrateIdentity(String existingId, String newId);

    /**
     * Deletes an identity.  This should NOT be called under normal circumstances and is in place to support internal
     * testing.  This method deletes any record that the identity exists, leaving open the following possible vectors:
     *
     * <ol>
     *     <li>The ID can be recreated, allowing the previous user to access using the ID.</li>
     *     <li>A typical implementation stores the identity using a hash of the ID.  If another identity is created
     *         whose ID hashes the same value then the previous users's ID would authenticate as the new ID.</li>
     *     <li>As more of the system associates first order objects with owners there is an increased risk for
     *         dangling identity references if they are deleted.</li>
     * </ol>
     *
     * Under normal circumstances the correct approach is to set an identity's state to INACTIVE using
     * {@link #updateIdentity(AuthIdentity)} rather than to delete the identity.
     */
    void deleteIdentityUnsafe(String id);

    /**
     * Returns an {@link InternalIdentity} view of the identity identified by internal ID, or null if no
     * such entity exists.  This method is useful when internal operations are required for a user, such as
     * verifying whether she has specific permissions, without actually logging the user in or exposing sufficient
     * information for the caller to log the user in or spoof that user's identity.
     */
    InternalIdentity getInternalIdentity(String internalId);
}
