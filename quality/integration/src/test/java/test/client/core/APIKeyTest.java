package test.client.core;

import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.client.DataStoreAuthenticator;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyResponse;
import com.bazaarvoice.emodb.uac.api.EmoApiKey;
import com.bazaarvoice.emodb.uac.api.EmoApiKeyExistsException;
import com.bazaarvoice.emodb.uac.api.EmoApiKeyNotFoundException;
import com.bazaarvoice.emodb.uac.api.EmoRole;
import com.bazaarvoice.emodb.uac.api.EmoRoleKey;
import com.bazaarvoice.emodb.uac.api.InvalidEmoPermissionException;
import com.bazaarvoice.emodb.uac.api.UpdateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UserAccessControl;
import com.beust.jcommander.internal.Sets;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;
import test.client.commons.TestModuleFactory;
import test.client.commons.annotations.ApiKeyTestDataStore;
import test.client.commons.utils.DataStoreHelper;
import test.client.commons.utils.UserAccessControlUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static test.client.commons.utils.RetryUtils.withRetryOnServiceUnavailable;
import static test.client.commons.utils.TableUtils.getAudit;
import static test.client.commons.utils.TableUtils.getTemplate;
import static test.client.commons.utils.UserAccessControlUtils.KEY_DESC;
import static test.client.commons.utils.UserAccessControlUtils.KEY_OWNER;
import static test.client.commons.utils.UserAccessControlUtils.addRolePermissions;
import static test.client.commons.utils.UserAccessControlUtils.addRolesToApiKey;
import static test.client.commons.utils.UserAccessControlUtils.apiKeyExistsByKey;
import static test.client.commons.utils.UserAccessControlUtils.createApiKey;
import static test.client.commons.utils.UserAccessControlUtils.createRole;
import static test.client.commons.utils.UserAccessControlUtils.deleteApiKey;
import static test.client.commons.utils.UserAccessControlUtils.deleteRole;
import static test.client.commons.utils.UserAccessControlUtils.getRoleKeyFromId;
import static test.client.commons.utils.UserAccessControlUtils.keysCleanup;
import static test.client.commons.utils.UserAccessControlUtils.migrateKey;
import static test.client.commons.utils.UserAccessControlUtils.revokeRolePermissions;
import static test.client.commons.utils.UserAccessControlUtils.roleExists;
import static test.client.commons.utils.UserAccessControlUtils.viewApiKeyByKey;
import static test.client.commons.utils.UserAccessControlUtils.viewRole;

@Test(timeOut = 360000)
@Guice(moduleFactory = TestModuleFactory.class)
public class APIKeyTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(APIKeyTest.class);
    private static final String APIKEY_TEST_CLIENT_NAME = "gatekeeper_apikey_client";

    private static final String BLOB_STANDARD_KEY = UserAccessControlUtils.padKey("BlobApiKeyStandard");
    private static final String BLOB_ADMIN_KEY = UserAccessControlUtils.padKey("BlobApiKeyAdmin");
    private static final String SOR_STANDARD_KEY = UserAccessControlUtils.padKey("SorApiKeyStandard");
    private static final String SOR_ADMIN_KEY = UserAccessControlUtils.padKey("SorApiKeyAdmin");
    private static final String SOR_UPDATE_KEY = UserAccessControlUtils.padKey("SorApiKeyUpdate");
    private static final String ADMIN_KEY = UserAccessControlUtils.padKey("ApiKeyAdmin");

    private final static String BASIC_ROLE = "sor_standard";

    private static final Map<String, String> KEY_ROLE_MAP = new HashMap<String, String>() {{
        put(BLOB_STANDARD_KEY, "blob_standard");
        put(BLOB_ADMIN_KEY, "blob_admin");
        put(SOR_STANDARD_KEY, "sor_standard");
        put(SOR_ADMIN_KEY, "sor_admin");
        put(SOR_UPDATE_KEY, "sor_update");
        put(ADMIN_KEY, "admin");
    }};

    @Inject
    @ApiKeyTestDataStore
    private Provider<DataStore> apiKeyTestDataStore;

    @Inject
    private DataStoreAuthenticator dataStoreAuthenticator;

    @Inject
    private DataStore dataStore;

    @Inject
    @Named("placement")
    private String placement;

    @Inject
    @Named("runID")
    private String runID;

    @Inject
    private UserAccessControl uac;

    private Set<String> tablesToCleanupAfterTest;

    private enum Action {
        CREATE,
        UPDATE,
        DROP,
    }

    @BeforeClass(alwaysRun = true, timeOut = 600000)
    public void createKeys() {
        for (String key : KEY_ROLE_MAP.keySet()) {
            int retry = 5;
            do {
                try {
                    createKey(uac, key, KEY_ROLE_MAP.get(key));
                    retry = 0;
                } catch (Exception e) {
                    retry--;
                    LOGGER.warn(String.format("Exception retry: %s", retry), e);
                }
            } while (retry > 0);
        }
    }

    @BeforeTest(alwaysRun = true)
    public void beforeTest() {
        tablesToCleanupAfterTest = new HashSet<>();
    }

    @AfterTest(alwaysRun = true)
    public void afterTest() {
        tablesToCleanupAfterTest.forEach((name) -> {
            LOGGER.info("Deleting sor table: {}", name);
            try {
                dataStore.dropTable(name, getAudit("drop table " + name));
            } catch (Exception e) {
                LOGGER.warn("Error to delete sor table: " + name, e);
            }
        });
    }

    @AfterClass
    public void deleteKeys() {
        keysCleanup(uac);
    }

    /*********************************************************************************************
     * Data providers
     *********************************************************************************************/
    @DataProvider
    public Object[][] dataProviderRoles() {
        return new Object[][]{
                // Parameters are [role name, permission set]
                // Note that roles are automatically added to group <code>roleGroup<code>
                {"trial", ImmutableSet.of("databus|*|ermacs_*", "queue|poll|ermacs_*", "sor|*|if(and({..,\"team\":\"ermacs\"},intrinsic(\"~placement\":\"ugc_global:ugc\")))")},
                {"fullDatabus", ImmutableSet.of("databus|*")}
        };
    }

    @DataProvider
    public Object[][] dataProviderBadRoles() {
        return new Object[][]{
                {"emptyPermit", ""},
                {"spacePermit", "  "},
                {"badSor", "sor|*|if(\"{)"},
        };
    }

    @DataProvider
    public Object[][] dataProviderTestPermission_sor() {
        final String baseName = "permission_test";
        String tableName = APIKEY_TEST_CLIENT_NAME.toLowerCase() + ":" + baseName;
        final String key = "sor_permissions";

        // Setting up some tables for UPDATE api key tests
        Delta delta = Deltas.fromString("{..,\"update_action\":1}");
        UUID uuid = TimeUUIDs.uuidForTimeMillis(System.currentTimeMillis());

        String tableName_1 = APIKEY_TEST_CLIENT_NAME + ":update_table_" + TimeUUIDs.newUUID();
        final DataStore dataStore = apiKeyTestDataStore.get();
        createDataTable(tableName_1, getTemplate("update_table_1", APIKEY_TEST_CLIENT_NAME, runID), placement);
        dataStore.update(tableName_1, key, uuid, delta, new AuditBuilder().setProgram("TestNG").setComment("update_" + uuid).setLocalHost().build());

        String tableName_2 = APIKEY_TEST_CLIENT_NAME + ":update_table_2_" + TimeUUIDs.newUUID();
        createDataTable(tableName_2, getTemplate("update_table_2", APIKEY_TEST_CLIENT_NAME, runID), placement);
        dataStore.update(tableName_2, key, uuid, delta, new AuditBuilder().setProgram("TestNG").setComment("update_" + uuid).setLocalHost().build());

        String tableName_3 = "update_key:update_table_" + TimeUUIDs.newUUID();
        createDataTable(tableName_3, getTemplate("update_table_3", APIKEY_TEST_CLIENT_NAME, runID), placement);
        dataStore.update(tableName_3, key, uuid, delta, new AuditBuilder().setProgram("TestNG").setComment("update_" + uuid).setLocalHost().build());

        String tableName_4 = "test_table:nothing_" + TimeUUIDs.newUUID();
        createDataTable(tableName_4, getTemplate("update_table_4", APIKEY_TEST_CLIENT_NAME, runID), placement);
        dataStore.update(tableName_4, key, uuid, delta, new AuditBuilder().setProgram("TestNG").setComment("update_" + uuid).setLocalHost().build());

        return new Object[][]{
                // Only specify one of apiKey or newKeyPermissions.  If apiKey is provided the test will use that API key,
                // otherwise it will create one with the provided permissions.

                //CREATE
                //apiKey, newKeyPermissions, action, table, expectedException
                {SOR_UPDATE_KEY, null, Action.CREATE, tableName, true},
                {null, ImmutableSet.of("sor|update|" + APIKEY_TEST_CLIENT_NAME + "*"), Action.CREATE, tableName, true},
                {BLOB_STANDARD_KEY, null, Action.CREATE, tableName, true},
                {BLOB_ADMIN_KEY, null, Action.CREATE, tableName, true},

                {SOR_STANDARD_KEY, null, Action.CREATE, tableName, false}, //apiTables.size = 1
                {null, ImmutableSet.of("sor|*|" + APIKEY_TEST_CLIENT_NAME + "*"), Action.CREATE, tableName, false}, //apiTables.size = 2
                {null, ImmutableSet.of("sor|create_table|" + APIKEY_TEST_CLIENT_NAME.toLowerCase() + "*", "sor|read|" + APIKEY_TEST_CLIENT_NAME.toLowerCase() + "*"), Action.CREATE, tableName, false}, //apiTables.size = 3
                {null, ImmutableSet.of("sor|*"), Action.CREATE, tableName_1, false},

                //UPDATE
                //apiKey, newKeyPermissions, action, table, expectedException
                {null, ImmutableSet.of("sor|update|" + APIKEY_TEST_CLIENT_NAME.toLowerCase() + "*"), Action.UPDATE, tableName_4, true},
                {null, ImmutableSet.of("sor|update|" + APIKEY_TEST_CLIENT_NAME.toLowerCase() + "*"), Action.UPDATE, tableName_3, true},

                {SOR_ADMIN_KEY, null, Action.UPDATE, tableName_1, false},
                {SOR_UPDATE_KEY, null, Action.UPDATE, tableName_2, false},
                {null, ImmutableSet.of("sor|update|" + APIKEY_TEST_CLIENT_NAME.toLowerCase() + "*"), Action.UPDATE, tableName_1, false},
                {null, ImmutableSet.of("sor|update|*_table*"), Action.UPDATE, tableName_1, false},
                {null, ImmutableSet.of("sor|update|*_table*"), Action.UPDATE, tableName_2, false},
                {null, ImmutableSet.of("sor|update|*_table*"), Action.UPDATE, tableName_3, false},
                {null, ImmutableSet.of("sor|update|*_table*"), Action.UPDATE, tableName_4, false},
                {SOR_UPDATE_KEY, null, Action.UPDATE, tableName_3, false},

                //DROP
                //apiKey, newKeyPermissions, action, table, expectedException
                {SOR_UPDATE_KEY, null, Action.DROP, tableName_1, true},
                {SOR_STANDARD_KEY, null, Action.DROP, tableName_2, true},

                {SOR_ADMIN_KEY, null, Action.DROP, tableName_1, false},
                {null, ImmutableSet.of("sor|drop_table|*"), Action.DROP, tableName_2, false},
                {ADMIN_KEY, null, Action.DROP, tableName_3, true},
                {ADMIN_KEY, null, Action.DROP, tableName_4, true}
        };
    }

    /*********************************************************************************************
     * Rest client tests
     *********************************************************************************************/
    @Test
    public void testViewUnknownKey() {
        String fakeKey = UserAccessControlUtils.padKey("SomeAnonymousApiKey");
        assertNull(viewApiKeyByKey(uac, fakeKey));
    }

    @Test
    public void testViewGeneratedKey() {
        for (String key : KEY_ROLE_MAP.keySet()) {
            checkCreatedKey(uac, key, KEY_ROLE_MAP.get(key));
        }
    }

    @Test(expectedExceptions = EmoApiKeyExistsException.class)
    public void testCreateKey() {
        String key = UserAccessControlUtils.padKey("testCreateApiKey");

        CreateEmoApiKeyResponse response = createKey(uac, key, BASIC_ROLE);
        assertEquals(response.getKey(), key, "Response did not have key '" + key + "'");
        checkCreatedKey(uac, key, BASIC_ROLE);

        CreateEmoApiKeyRequest request = new CreateEmoApiKeyRequest()
                .setOwner(KEY_OWNER)
                .setDescription(KEY_DESC)
                .setRoles(new HashSet<EmoRoleKey>() {{
                    add(getRoleKeyFromId(BASIC_ROLE));
                }});

        // To create a specific, non-random API key requires the following custom parameter
        request.setCustomRequestParameter("key", key);
        uac.createApiKey(request);
        fail("EmoApiKeyExistsException not thrown");
    }

    @Test
    public void testUpdateKey() {
        String key = UserAccessControlUtils.padKey("testUpdateApiKey");

        CreateEmoApiKeyResponse response = createKey(uac, key, BASIC_ROLE);
        assertEquals(response.getKey(), key, "Response did not have key '" + key + "'");
        String id = response.getId();
        checkCreatedKey(uac, key, BASIC_ROLE);

        String updated_role = "sor_update";
        updateApiKey(uac, id, ImmutableSet.of(updated_role), ImmutableSet.of(BASIC_ROLE));
        checkCreatedKey(uac, key, updated_role);

        addRolesToApiKey(uac, id, ImmutableSet.of(BASIC_ROLE));
        checkCreatedKey(uac, key, BASIC_ROLE);
        checkCreatedKey(uac, key, updated_role);
    }

    @Test
    public void testMigrate() {
        String key = UserAccessControlUtils.padKey("testMigrateApiKey");
        assertFalse(apiKeyExistsByKey(uac, key), key + " exists before test!");
        CreateEmoApiKeyResponse response = createKey(uac, key, BASIC_ROLE);
        checkCreatedKey(uac, key, BASIC_ROLE);
        String new_key = migrateKey(uac, key);
        checkCreatedKey(uac, new_key, BASIC_ROLE);
    }

    @Test(expectedExceptions = EmoApiKeyNotFoundException.class)
    public void testDeleteKey() {
        String key = UserAccessControlUtils.padKey("FakeApiKeyAdmin");

        assertFalse(apiKeyExistsByKey(uac, key), key + " exists before test!");

        CreateEmoApiKeyResponse response = createKey(uac, key, BASIC_ROLE);
        String id = response.getId();
        assertTrue(apiKeyExists(uac, id), id + " does not exists!");
        assertTrue(apiKeyExistsByKey(uac, key), key + " does not exists!");
        deleteApiKey(uac, id);
        assertFalse(apiKeyExists(uac, id), id + " exists after delete!");
        assertFalse(apiKeyExistsByKey(uac, key), key + " exists after delete!");

        deleteApiKey(uac, id);
        fail("EmoApiKeyNotFoundException not thrown");
    }

    @Test(dataProvider = "dataProviderRoles")
    public void testCreateRole(String roleName, Set<String> permissions) {
        createRole(uac, roleName, permissions);
    }

    @Test(dataProvider = "dataProviderRoles", dependsOnMethods = "testCreateRole")
    public void testViewRole(String roleName, Set<String> permissions) {
        EmoRole role = viewRole(uac, roleName);
        assertNotNull(role, "role not found");
        assertEquals(role.getPermissions(), permissions, "Permissions do not match");
    }

    @Test
    public void testUpdateRole() {
        String roleName = "updateRole";
        Set<String> permissions = ImmutableSet.of("databus|*", "queue|poll|ermacs_*");
        createRole(uac, roleName, permissions);

        revokeRolePermissions(uac, roleName, ImmutableSet.of("databus|*"));
        EmoRole role = viewRole(uac, roleName);
        assertEquals(role.getPermissions(), ImmutableSet.of("queue|poll|ermacs_*"));

        addRolePermissions(uac, roleName, ImmutableSet.of("sor|*"));
        role = viewRole(uac, roleName);
        assertEquals(role.getPermissions(), ImmutableSet.of("queue|poll|ermacs_*", "sor|*"));
    }

    @Test(dataProvider = "dataProviderRoles", dependsOnMethods = "testViewRole")
    public void testDeleteRole(String roleName, Set<String> permissions) {
        assertTrue(roleExists(uac, roleName), roleName + " does not exists!");
        deleteRole(uac, roleName);
        assertFalse(roleExists(uac, roleName), roleName + " still exists!");
    }

    @Test(dataProvider = "dataProviderBadRoles", expectedExceptions = InvalidEmoPermissionException.class)
    public void negativeTestCreateRole(String roleName, String permission) {
        createRole(uac, roleName, ImmutableSet.of(permission));
    }

    @Test(dataProvider = "dataProviderTestPermission_sor")
    public void testSufficientPermission_sor(String apiKey, Set<String> newKeyPermissions, Action action, String table, boolean expectedException) {
        assertTrue((apiKey == null) != (newKeyPermissions == null), "Exactly one of apiKey or newKeyPermissions should be provided");

        LOGGER.debug("testSufficientPermission_sor - Parameters: {} {} {} {} {}", apiKey, newKeyPermissions, action, table, expectedException);

        if (apiKey == null) {
            String roleName = "customRole";
            apiKey = "customKey000000000000000000000000000000000000000";
            if (roleExists(uac, roleName)) {
                deleteRole(uac, roleName); // So the the role only has the permissions needed for the test
            }
            EmoApiKey existingKey = viewApiKeyByKey(uac, apiKey);
            if (existingKey != null) {
                deleteApiKey(uac, existingKey.getId());
            }
            createRole(uac, roleName, newKeyPermissions);
            assertTrue(roleExists(uac, roleName), roleName + " was not created!");
            CreateEmoApiKeyResponse response = createKey(uac, apiKey, roleName);
            assertTrue((apiKeyExistsByKey(uac, apiKey)), apiKey + " was not created!");
        }

        final DataStore dataStore = dataStoreAuthenticator.usingCredentials(apiKey);
        final DataStore dataStore1 = apiKeyTestDataStore.get();

        switch (action) {
            case CREATE:
                table = table + "_" + TimeUUIDs.newUUID();
                try {
                    createDataTable(table, getTemplate("api_permissions", APIKEY_TEST_CLIENT_NAME, runID), placement);
                } catch (Exception e) {
                    if (expectedException) {
                        assertTrue(e instanceof UnauthorizedException, "Proper exception not thrown in CREATE!");
                    } else {
                        LOGGER.error("Exception: ", e);
                        fail("Unexpected Exception: " + e.toString() + "\nTable: " + table + "\nAPI Key: " + apiKey + "\nPermissions: " + newKeyPermissions);
                    }
                }
                tablesToCleanupAfterTest.add(table);

                break;
            case UPDATE:
                try {
                    Random rand = new Random();
                    int randomInt = rand.nextInt();

                    Delta delta = Deltas.fromString("{..,\"update_action\":" + randomInt + "}");
                    UUID uuid = TimeUUIDs.uuidForTimeMillis(System.currentTimeMillis());
                    dataStore.update(table, "update_action", uuid, delta, new AuditBuilder().setProgram("TestNG").setComment("update_action_" + randomInt + "; update_" + uuid).setLocalHost().build());
                    Map<String, Object> result = dataStore1.get(table, "update_action");
                    assertEquals(Integer.parseInt(result.get("update_action").toString()), randomInt, "Update did not occure");
                } catch (Exception e) {
                    if (expectedException) {
                        assertTrue(e instanceof UnauthorizedException, "Proper exception not thrown in UPDATE!");
                    } else {
                        LOGGER.error("Exception: ", e);
                        fail("Unexpected Exception: " + e.toString() + "\nTable: " + table + "\nAPI Key: " + apiKey + "\nPermissions: " + newKeyPermissions);
                    }
                }
                break;
            case DROP:
                assertTrue(dataStore1.getTableExists(table), table + " does not exists!");
                try {
                    dataStore.dropTable(table, getAudit("drop table" + table));
                    if (expectedException) {
                        fail("Proper exception not thrown in DROP!");
                    } else {
                        assertFalse(dataStore1.getTableExists(table), table + " still exists!");
                    }
                } catch (Exception e) {
                    if (expectedException) {
                        assertTrue(dataStore1.getTableExists(table), table + " does not exists!");
                    } else {
                        fail("Exception thrown in DROP!");
                    }
                }
                break;
        }
    }

    private static void updateApiKey(UserAccessControl uac, String id, Set<String> addRoles, Set<String> removeRoles) {
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

    private static CreateEmoApiKeyResponse createKey(UserAccessControl uac, String key, String role) {
        return createApiKey(uac, key, KEY_OWNER, KEY_DESC, ImmutableSet.of(role));
    }

    private static boolean apiKeyExists(UserAccessControl uac, String id) {
        return uac.getApiKey(id) != null;
    }


    private static void checkCreatedKey(UserAccessControl uac, String key, String role) {
        EmoApiKey emoApiKey = viewApiKeyByKey(uac, key);
        assertNotNull(emoApiKey, "key not found");
        assertEquals(emoApiKey.getOwner(), KEY_OWNER, "key_owner not found: " + emoApiKey);
        assertEquals(emoApiKey.getDescription(), KEY_DESC, "key_desc not found" + emoApiKey);
        assertTrue(emoApiKey.getRoles().contains(getRoleKeyFromId(role)), "role: " + role + " not found: roles=" + emoApiKey.getRoles());
    }

    private void createDataTable(String tableName, Map<String, String> template, String placement) {
        LOGGER.info("Creating sor table: {}", tableName);
        DataStoreHelper.createDataTable(dataStore, tableName, template, placement);
        tablesToCleanupAfterTest.add(tableName);
    }
}
