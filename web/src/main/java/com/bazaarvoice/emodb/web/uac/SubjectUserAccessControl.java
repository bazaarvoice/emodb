package com.bazaarvoice.emodb.web.uac;

import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyResponse;
import com.bazaarvoice.emodb.uac.api.CreateEmoRoleRequest;
import com.bazaarvoice.emodb.uac.api.EmoApiKey;
import com.bazaarvoice.emodb.uac.api.EmoRole;
import com.bazaarvoice.emodb.uac.api.EmoRoleKey;
import com.bazaarvoice.emodb.uac.api.MigrateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UpdateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UpdateEmoRoleRequest;

import java.util.Iterator;

/**
 * Interface similar to {@link com.bazaarvoice.emodb.uac.api.AuthUserAccessControl}, with the following differences:
 *
 * <ol>
 *     <li>
 *         Because this interface exists to service server-side UAC requests users are represented by {@link Subject}
 *         instead of API key.  This allows for either servicing the request locally or, if necessary for global
 *         consistency, forwarding to another data center.
 *     </li>
 *     <li>
 *         Because this interface is not part of a public API it does not need to provide an exact mirror of
 *         AuthUserAccessControl.  There is no requirement that the two remain synchronized, only that this interface
 *         can service all requests from AuthUserAccessControl.
 *     </li>
 * </ol>
 */
public interface SubjectUserAccessControl {
    Iterator<EmoRole> getAllRoles(Subject subject);

    Iterator<EmoRole> getAllRolesInGroup(Subject subject, String group);

    EmoRole getRole(Subject subject, EmoRoleKey roleKey);

    void createRole(Subject subject, CreateEmoRoleRequest request);

    void updateRole(Subject subject, UpdateEmoRoleRequest request);

    void deleteRole(Subject subject, EmoRoleKey roleKey);

    boolean checkRoleHasPermission(Subject subject, EmoRoleKey roleKey, String permission);

    EmoApiKey getApiKeyByKey(Subject subject, String key);

    EmoApiKey getApiKey(Subject subject, String id);

    CreateEmoApiKeyResponse createApiKey(Subject subject, CreateEmoApiKeyRequest request);

    void updateApiKey(Subject subject, UpdateEmoApiKeyRequest request);

    String migrateApiKey(Subject subject, MigrateEmoApiKeyRequest request);

    void deleteApiKey(Subject subject, String id);

    boolean checkApiKeyHasPermission(Subject subject, String id, String permission);
}
