package com.bazaarvoice.emodb.uac.api;

import java.util.Iterator;

/**
 * Simplified interface for {@link AuthUserAccessControl}.  Each method in this interface is a duplicate of the
 * equivalent AuthUserAccessControl method with the parameter for the caller's API key removed.  The expectation is that
 * an implementation would be constructed with an API key.  The implementation would then delegate all calls to an
 * AuthUserAccessControl, thus making passing the API key to every call unnecessary.
 *
 * @see AuthUserAccessControl
 */

public interface UserAccessControl {

    /**
     * @see AuthUserAccessControl#getAllRoles(String)
     */
    Iterator<EmoRole> getAllRoles();

    /**
     * @see AuthUserAccessControl#getAllRolesInGroup(String, String)
     */
    Iterator<EmoRole> getAllRolesInGroup(String group);

    /**
     * @see AuthUserAccessControl#getRole(String, EmoRoleKey)
     */
    EmoRole getRole(EmoRoleKey roleKey);

    /**
     * @see AuthUserAccessControl#createRole(String, CreateEmoRoleRequest)
     */
    void createRole(CreateEmoRoleRequest request) throws EmoRoleExistsException;

    /**
     * @see AuthUserAccessControl#updateRole(String, UpdateEmoRoleRequest)
     */
    void updateRole(UpdateEmoRoleRequest request) throws EmoRoleNotFoundException;

    /**
     * @see AuthUserAccessControl#deleteRole(String, EmoRoleKey)
     */
    void deleteRole(EmoRoleKey roleKey) throws EmoRoleNotFoundException;

    /**
     * @see AuthUserAccessControl#checkRoleHasPermission(String, EmoRoleKey, String)
     */
    boolean checkRoleHasPermission(EmoRoleKey roleKey, String permission) throws EmoRoleNotFoundException;

    /**
     * @see AuthUserAccessControl#getApiKey(String, String)
     */
    EmoApiKey getApiKey(String id);

    /**
     * @see AuthUserAccessControl#getApiKeyByKey(String, String)
     */
    EmoApiKey getApiKeyByKey(String key);

    /**
     * @see AuthUserAccessControl#createApiKey(String, CreateEmoApiKeyRequest)
     */
    CreateEmoApiKeyResponse createApiKey(CreateEmoApiKeyRequest request);

    /**
     * @see AuthUserAccessControl#updateApiKey(String, UpdateEmoApiKeyRequest)
     */
    void updateApiKey(UpdateEmoApiKeyRequest request) throws EmoApiKeyNotFoundException;

    /**
     * @see AuthUserAccessControl#migrateApiKey(String, String)
     */
    String migrateApiKey(String id) throws EmoApiKeyNotFoundException;

    /**
     * @see AuthUserAccessControl#migrateApiKey(String, MigrateEmoApiKeyRequest)
     */
    String migrateApiKey(MigrateEmoApiKeyRequest request) throws EmoApiKeyNotFoundException;

    /**
     * @see AuthUserAccessControl#deleteApiKey(String, String) 
     */
    void deleteApiKey(String id) throws EmoApiKeyNotFoundException;

    /**
     * @see AuthUserAccessControl#checkApiKeyHasPermission(String, String, String) 
     */
    boolean checkApiKeyHasPermission(String id, String permission) throws EmoApiKeyNotFoundException;
}
