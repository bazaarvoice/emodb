package com.bazaarvoice.emodb.uac.api;

import com.bazaarvoice.emodb.auth.proxy.Credential;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;

import java.util.Iterator;

/**
 * Interface for user access control.  The methods in this interface are used for managing users, roles, and permissions.
 * Note that use of these methods require special permissions from EmoDB.  If a caller invokes a method for which he has
 * insufficient permissions the method will throw an {@link UnauthorizedException}.  Note that this exception takes
 * precedence over any other exceptions or responses.  For example, assume a caller does not have permission to view
 * any roles.  When that caller makes a call to {@link #getRole(String, EmoRoleKey)} he will always get an
 * UnauthorizedException, regardless of whether the role he's getting actually exists or not.
 *
 * The first parameter to every call is the API key of the caller.  It is used for authenticating the caller and
 * verifying his credentials.  Methods which manage API keys operate on the API key's ID, unless otherwise noted,
 * and those parameters always appear after the caller's API key.
 */
public interface AuthUserAccessControl {

    /**
     * Gets all roles for which the user has read access.  This method will not throw an UnauthorizedException if
     * a valid API key is used but does not have permission to view any roles.  In that case it will return
     * an empty iterable.
     */
    Iterator<EmoRole> getAllRoles(@Credential String apiKey);

    /**
     * Gets all roles in the provided group for which the user has read access.   This method will not throw an
     * UnauthorizedException if a valid API key is used but does not have permission to view any roles in the group.
     * In that case it will return an empty iterable.
     */
    Iterator<EmoRole> getAllRolesInGroup(@Credential String apiKey, String group);

    /**
     * Gets a role, or returns null if the role doesn't exist.
     * @throws UnauthorizedException Caller doesn't have permission to view this role.
     */
    EmoRole getRole(@Credential String apiKey, EmoRoleKey roleKey);

    /**
     * Creates a role.
     * @throws EmoRoleExistsException Another role with the same key already exists
     * @throws UnauthorizedException Caller doesn't have permission to create this role
     */
    void createRole(@Credential String apiKey, CreateEmoRoleRequest request)
            throws EmoRoleExistsException;

    /**
     * Updates a role.
     * @throws EmoRoleNotFoundException No role with the provided key exists
     * @throws UnauthorizedException Caller doesn't have permission to update this role
     */
    void updateRole(@Credential String apiKey, UpdateEmoRoleRequest request)
            throws EmoRoleNotFoundException;

    /**
     * Deletes a role.
     * @throws EmoRoleNotFoundException No role with the provided key exists
     * @throws UnauthorizedException Caller doesn't have permission to delete this role
     */
    void deleteRole(@Credential String apiKey, EmoRoleKey roleKey)
            throws EmoRoleNotFoundException;

    /**
     * Gets an API key by its ID, or null if the API key doesn't exist.
     * @throws UnauthorizedException Caller doesn't have permission to view this key
     */
    EmoApiKey getApiKey(@Credential String apiKey, String id);

    /**
     * Gets an API key by the private key, or null if the API key doesn't exist.  Because of the potential for a
     * malicious caller to use this method to brute-force search for valid API keys permission to make this call is
     * rarely granted.  Most users should expect to get an UnauthorizedException from this call, even if they provide a
     * valid key (including their own key).
     * @throws UnauthorizedException Caller doesn't have permission to view this key
     */
    EmoApiKey getApiKeyByKey(@Credential String apiKey, String key);

    /**
     * Creates a new API key.  The response includes both the private key and the key's ID.  There are two
     * separate permissions which are needed when creating API keys:  the permission to create API keys, and,
     * for each role in the request, permission to assign that role to the key.
     *
     * Because the response includes a private key permission to make this call is rarely granted.  Most users
     * should expect to get an UnauthorizedException from this call.
     *
     * @throws UnauthorizedException Caller either doesn't have permission to create API keys or to assign
     *                               one of the roles
     */
    CreateEmoApiKeyResponse createApiKey(@Credential String apiKey, CreateEmoApiKeyRequest request);

    /**
     * Updates an API key.  As with API key creation, the caller must have permission to update the API key and,
     * if any roles are added or removed, permission to add or remove those roles.  Additionally, if the
     * request returns true from {@link UpdateEmoApiKeyRequest#isUnassignOtherRoles()} then the caller must also
     * have permission to remove any roles already assigned to the key which would be removed as a result.
     *
     * @throws EmoApiKeyNotFoundException No API key with the provided ID exists
     * @throws UnauthorizedException Caller either doesn't have permission to update the API key or to
     *                               assign or unassign one of the roles
     */
    void updateApiKey(@Credential String apiKey, UpdateEmoApiKeyRequest request)
            throws EmoApiKeyNotFoundException;

    /**
     * Convenience call to the most common use of {@link #migrateApiKey(String, MigrateEmoApiKeyRequest)}.
     * Equivalent to <code>migrateApiKey(apiKey, new MigrateApiKeyRequest(id))</code>
     * @see #migrateApiKey(String, MigrateEmoApiKeyRequest)
     */
    String migrateApiKey(@Credential String apiKey, String id)
            throws EmoApiKeyNotFoundException;

    /**
     * Migrates an existing API key to a new API key.  Only the private key changes upon migration; the key's ID,
     * attributes, and assigned roles all remain the same.  Any API calls made using the old key will no longer
     * authenticate after it has been migrated.  This method is typically used either when a caller lost his private
     * key or when the key has definitively or potentially been leaked.  This method returns the new private key.
     *
     * Because of its ability to disrupt service availability for valid API keys permission to make this call is
     * rarely granted.  Most users should expect to get an UnauthorizedException from this call.
     *
     * @throws EmoApiKeyNotFoundException No API key with the provided ID exists
     * @throws UnauthorizedException Caller doesn't have permission to migrate the API key
     */    String migrateApiKey(@Credential String apiKey, MigrateEmoApiKeyRequest request)
            throws EmoApiKeyNotFoundException;

    /**
     * Deletes an API key.  The caller must have permission to delete the API key and, if the key has any roles assigned,
     * permission to remove those roles.
     *
     * Because of its ability to disrupt service availability for valid API keys permission to make this call is
     * rarely granted.  Most users should expect to get an UnauthorizedException from this call.
     *
     * @throws UnauthorizedException Caller either doesn't have permission to delete the API key or to
     *                               unassign one of the currently assigned roles
     */
    void deleteApiKey(@Credential String apiKey, String id)
            throws EmoApiKeyNotFoundException;     
}
