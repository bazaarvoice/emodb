package com.bazaarvoice.gatekeeper.emodb.commons.utils;

import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyResponse;
import com.bazaarvoice.emodb.uac.api.CreateEmoRoleRequest;
import com.bazaarvoice.emodb.uac.api.EmoApiKey;
import com.bazaarvoice.emodb.uac.api.EmoRole;
import com.bazaarvoice.emodb.uac.api.EmoRoleKey;
import com.bazaarvoice.emodb.uac.api.UpdateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UpdateEmoRoleRequest;
import com.bazaarvoice.emodb.uac.api.UserAccessControl;
import com.beust.jcommander.internal.Sets;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.bazaarvoice.gatekeeper.emodb.commons.utils.RetryUtils.withRetryOnServiceUnavailable;

public class UserAccessControlUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserAccessControlUtils.class);

    public final static String KEY_OWNER = "emodb-dev@bazaarvoice.com";
    public final static String KEY_DESC = "data-team-qa_APITests";
    private final static String ROLE_GROUP = "emodb-gatekeeper";
    private static final BiMap<String, String> KEYS_TO_IDS = HashBiMap.create();
    private static final List<String> ROLES_CREATED = new ArrayList<>();

    /**
     * List of roles provided by Emo that are not created by Gatekeeper and do not belong to a role group.
     * This is not a complete list; as more are utilized by tests they should be added here.
     * Default Roles:
     * sor_update, sor_standard, sor_admin
     * blob_update, blob_standard, blob_admin
     * record_update, record_standard, record_admin
     * facade_admin
     * queue_post, queue_poll, queue_standard, queue_admin
     * databus_poll, databus_standard, databus_admin
     * standard
     * admin
     * replication
     * shovel
     * anonymous
     */
    private static final Set<String> UNMANAGED_ROLES = ImmutableSet.copyOf(new String[]{
            "blob_standard", "blob_admin", "sor_standard", "sor_admin", "sor_update", "databus_standard"
    });

    public static EmoApiKey viewApiKeyByKey(UserAccessControl uac, String key) {
        return uac.getApiKeyByKey(key);
    }

    /**
     * Convenience method which returns a role in the default role group for unmanaged roles and the {@link #ROLE_GROUP}
     * group for Gatekeeper managed roles.
     */
    public static EmoRoleKey getRoleKeyFromId(String role) {
        if (UNMANAGED_ROLES.contains(role)) {
            return new EmoRoleKey(null, role);
        }
        return new EmoRoleKey(ROLE_GROUP, role);
    }

    public static CreateEmoApiKeyResponse createApiKey(UserAccessControl uac, String key, String owner, String description, Set<String> roles) {
        LOGGER.debug("Creating Key: " + key);
        Set<EmoRoleKey> roleKeys = Sets.newLinkedHashSet();
        for (String role : roles) {
            roleKeys.add(getRoleKeyFromId(role));
        }

        CreateEmoApiKeyRequest request = new CreateEmoApiKeyRequest()
                .setOwner(owner)
                .setDescription(description)
                .setRoles(roleKeys);

        // To create a specific, non-random API key requires the following custom parameter
        request.setCustomRequestParameter("key", key);
        CreateEmoApiKeyResponse response;
        if (apiKeyExistsByKey(uac, key)) {
            response = new CreateEmoApiKeyResponse(key, viewApiKeyByKey(uac, key).getId());
        } else {
            response = withRetryOnServiceUnavailable(() -> uac.createApiKey(request));
        }
        LOGGER.debug("Added {}:{}", response.getKey(), response.getId());
        KEYS_TO_IDS.put(response.getKey(), response.getId());

        return response;
    }

    public static void updateApiKey(UserAccessControl uac, String id, Set<String> addRoles, Set<String> removeRoles) {
        Set<EmoRoleKey> addRoleKeys = Sets.newLinkedHashSet();
        Set<EmoRoleKey> removeRoleKeys = Sets.newLinkedHashSet();

        if (addRoles != null && !addRoles.isEmpty()) {
            for (String addRole : addRoles) {
                addRoleKeys.add(getRoleKeyFromId(addRole));
            }
        }
        if (removeRoles != null && !removeRoles.isEmpty()) {
            for (String removeRole : removeRoles) {
                removeRoleKeys.add(getRoleKeyFromId(removeRole));
            }
        }

        withRetryOnServiceUnavailable(() -> uac.updateApiKey(new UpdateEmoApiKeyRequest(id)
                .assignRoles(addRoleKeys)
                .unassignRoles(removeRoleKeys)));
    }

    public static void addRolesToApiKey(UserAccessControl uac, String id, Set<String> roles) {
        updateApiKey(uac, id, roles, null);
    }

    public static boolean apiKeyExistsByKey(UserAccessControl uac, String key) {
        EmoApiKey apiKey = uac.getApiKeyByKey(key);

        if (apiKey != null) {
            KEYS_TO_IDS.put(key, apiKey.getId());
            return true;
        }
        return false;
    }

    public static void deleteApiKey(UserAccessControl uac, String id) {
        withRetryOnServiceUnavailable(() -> uac.deleteApiKey(id));
    }

    public static String migrateKey(UserAccessControl uac, String key) {
        String id = KEYS_TO_IDS.get(key);
        String newKey = migrateApiKey(uac, id);
        KEYS_TO_IDS.inverse().replace(id, newKey);
        return newKey;
    }

    protected static String migrateApiKey(UserAccessControl uac, String id) {
        return withRetryOnServiceUnavailable(() -> uac.migrateApiKey(id));
    }

    /*********************************************************************************************
     * Role and API key methods.
     * All roles created by tests implicitly go into the {@link #ROLE_GROUP} group
     *********************************************************************************************/

    public static void createRole(UserAccessControl uac, String role, Set<String> permissions) {
        LOGGER.debug("Create Role: {}", role);
        if (roleExists(uac, role)) {
            deleteRole(uac, role);
        }
        withRetryOnServiceUnavailable(() -> uac.createRole(
                new CreateEmoRoleRequest(new EmoRoleKey(ROLE_GROUP, role))
                        .setName(role)
                        .setPermissions(permissions)));
        ROLES_CREATED.add(role);
    }

    public static void addRolePermissions(UserAccessControl uac, String role, Set<String> permits) {
        updateRole(uac, role, permits, ImmutableSet.of());
    }

    public static void revokeRolePermissions(UserAccessControl uac, String role, Set<String> revokes) {
        updateRole(uac, role, ImmutableSet.of(), revokes);
    }

    public static void updateRole(UserAccessControl uac, String role, Set<String> permits, Set<String> revokes) {
        withRetryOnServiceUnavailable(() -> uac.updateRole(
                new UpdateEmoRoleRequest(new EmoRoleKey(ROLE_GROUP, role))
                        .grantPermissions(permits)
                        .revokePermissions(revokes)));
    }

    public static boolean roleExists(UserAccessControl uac, String role) {
        return viewRole(uac, role) != null;
    }

    public static EmoRole viewRole(UserAccessControl uac, String role) {
        return withRetryOnServiceUnavailable(() -> uac.getRole(new EmoRoleKey(ROLE_GROUP, role)));
    }

    public static void deleteRole(UserAccessControl uac, String role) {
        withRetryOnServiceUnavailable(() -> uac.deleteRole(new EmoRoleKey(ROLE_GROUP, role)));
    }

    public static void keysCleanup(UserAccessControl uac) {
        for (String key : KEYS_TO_IDS.keySet()) {
            if (apiKeyExistsByKey(uac, key)) {
                EmoApiKey apiKey = viewApiKeyByKey(uac, key);
                int retry = 10;
                LOGGER.debug("Attempt {}. Deleting key - {}:{}", retry, key, apiKey.getId());
                do {
                    try {
                        deleteApiKey(uac, apiKey.getId());
                        retry = 0;
                    } catch (Exception e) {
                        retry--;
                        LOGGER.warn(String.format("Exception retry: %s", retry), e);
                    }
                } while (retry > 0);
                if (apiKeyExistsByKey(uac, key)) {
                    throw new IllegalStateException(key + " still exists after cleanup!");
                }
            }
        }

        for (String role : ROLES_CREATED) {
            if (roleExists(uac, role)) {
                deleteRole(uac, role);
                if (roleExists(uac, role)) {
                    throw new IllegalStateException(role + " still exists after cleanup!");
                }
            }
        }
    }

    public static String padKey(String key) {
        StringBuilder keyBuilder = new StringBuilder(key);
        while (keyBuilder.length() != 48) {
            keyBuilder.append("0");
        }
        return keyBuilder.toString();
    }
}
