package test.integration.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyModification;
import com.bazaarvoice.emodb.auth.identity.TableAuthIdentityManagerDAO;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

public class TableAuthIdentityManagerDAOTest {

    /**
     * There are two tables which store identities in TableAuthIdentityManagerDAO: One table keyed by a hash of the
     * API key, and an index table ID'd by the identity ID which contains the API key hash.  This second table is used
     * to look up API keys by ID.  It should be rare, but it is possible for an API key record tNAo exist
     * without a corresponding ID.  One possible way for this to happen is grandfathered in API keys
     * created before the introduction of IDs.  TableAuthIdentityManagerDAO should rebuild the index
     * when there is a missing or incorrect index record.  This test verifies that works as expected.
     */
    @Test
    public void testRebuildIdIndex() {
        DataStore dataStore = new InMemoryDataStore(new MetricRegistry());
        Supplier<String> idSupplier = () -> "id0";
        TableAuthIdentityManagerDAO<ApiKey> tableAuthIdentityManagerDAO = new TableAuthIdentityManagerDAO<>(
                ApiKey.class, dataStore, "__auth:keys", "__auth:internal_ids", "app_global:sys",
                idSupplier, Hashing.sha256());

        tableAuthIdentityManagerDAO.createIdentity("testkey",
                new ApiKeyModification()
                        .withOwner("testowner")
                        .addRoles("role1", "role2"));

        // Verify both tables have been written

        String keyTableId = Hashing.sha256().hashUnencodedChars("testkey").toString();

        Map<String, Object> keyMap = dataStore.get("__auth:keys", keyTableId);
        assertFalse(Intrinsic.isDeleted(keyMap));
        assertEquals(keyMap.get("owner"), "testowner");

        Map<String, Object> indexMap = dataStore.get("__auth:internal_ids", "id0");
        assertFalse(Intrinsic.isDeleted(indexMap));
        assertEquals(indexMap.get("hashedId"), keyTableId);

        // Deliberately delete the index map record
        dataStore.update("__auth:internal_ids", "id0", TimeUUIDs.newUUID(), Deltas.delete(),
                new AuditBuilder().setComment("test delete").build());

        Set<String> roles = tableAuthIdentityManagerDAO.getIdentity("id0").getRoles();
        assertEquals(roles, ImmutableSet.of("role1", "role2"));

        // Verify that the index record is re-created
        indexMap = dataStore.get("__auth:internal_ids", "id0");
        assertFalse(Intrinsic.isDeleted(indexMap));
        assertEquals(indexMap.get("hashedId"), keyTableId);
    }

    @Test
    public void testGrandfatheredInId() {
        DataStore dataStore = new InMemoryDataStore(new MetricRegistry());
        Supplier<String> idSupplier = () -> "id0";
        TableAuthIdentityManagerDAO<ApiKey> tableAuthIdentityManagerDAO = new TableAuthIdentityManagerDAO<>(
                ApiKey.class, dataStore, "__auth:keys", "__auth:internal_ids", "app_global:sys",
                idSupplier, Hashing.sha256());

        // Perform an operation on tableAuthIdentityManagerDAO to force it to create API key tables; the actual
        // operation doesn't matter.
        tableAuthIdentityManagerDAO.getIdentity("ignore");

        String id = "aaaabbbbccccddddeeeeffffgggghhhhiiiijjjjkkkkllll";
        String hash = Hashing.sha256().hashUnencodedChars(id).toString();

        // Write out a record which mimics the pre-internal-id format.  Notably missing is the "internalId" attribute.
        Map<String, Object> oldIdentityMap = ImmutableMap.<String, Object>builder()
                .put("maskedId", "aaaa****************************************llll")
                .put("owner", "someone")
                .put("description", "something")
                .put("roles", ImmutableList.of("role1", "role2"))
                .build();

        dataStore.update("__auth:keys", hash, TimeUUIDs.newUUID(), Deltas.literal(oldIdentityMap),
                new AuditBuilder().setComment("test grandfathering").build());

        for (int i=0; i < 2; i++) {
            ApiKey apiKey;
            if (i == 0) {
                // Verify the record can be read by authentication ID.  The key's ID will be the hashed ID.
                apiKey = tableAuthIdentityManagerDAO.getIdentityByAuthenticationId(id);
            } else {
                // Verify that a lookup by ID works
                apiKey = tableAuthIdentityManagerDAO.getIdentity(hash);
            }
            assertNotNull(apiKey);
            assertEquals(apiKey.getId(), hash);
            assertEquals(apiKey.getOwner(), "someone");
            assertEquals(apiKey.getDescription(), "something");
            assertEquals(apiKey.getRoles(), ImmutableList.of("role1", "role2"));
        }

        // Verify that the index record was created with the hashed ID as the ID
        Map<String, Object> indexMap = dataStore.get("__auth:internal_ids", hash);
        assertFalse(Intrinsic.isDeleted(indexMap));
        assertEquals(indexMap.get("hashedId"), hash);

        // Verify lookup by ID still works with the index record in place
        ApiKey apiKey = tableAuthIdentityManagerDAO.getIdentity(hash);
        assertEquals(apiKey.getRoles(), ImmutableSet.of("role1", "role2"));
    }

    @Test
    public void testIdAttributeCompatibility() {
        DataStore dataStore = new InMemoryDataStore(new MetricRegistry());
        Supplier<String> idSupplier = () -> "id0";
        TableAuthIdentityManagerDAO<ApiKey> tableAuthIdentityManagerDAO = new TableAuthIdentityManagerDAO<>(
                ApiKey.class, dataStore, "__auth:keys", "__auth:internal_ids", "app_global:sys",
                idSupplier, Hashing.sha256());

        // Perform an operation on tableAuthIdentityManagerDAO to force it to create API key tables; the actual
        // operation doesn't matter.
        tableAuthIdentityManagerDAO.getIdentity("ignore");

        List<String> keys = ImmutableList.of(
                "000000000000000000000000000000000000000000000000", "000000000000000000000000000000000000000000000001");
        List<String> hashes = keys.stream().map(id -> Hashing.sha256().hashUnencodedChars(id).toString()).collect(Collectors.toList());
        List<String> ids = ImmutableList.of("id0", "id1");

        // Write out two records: one with the legacy "internalId" and one with the forward-compatible "id" attribute
        for (int i=0; i < 2; i++) {
            String key = keys.get(i);
            String hash = hashes.get(i);
            String id = ids.get(i);

            Map<String, Object> map = ImmutableMap.<String, Object>builder()
                    .put("maskedId", "****")
                    .put("owner", "someone")
                    .put("description", "something")
                    .put("roles", ImmutableList.of("role1", "role2"))
                    .put(i == 0 ? "internalId" : "id", id)
                    .build();

            dataStore.update("__auth:keys", hash, TimeUUIDs.newUUID(), Deltas.literal(map),
                    new AuditBuilder().setComment("test grandfathering").build());

            for (int j = 0; j < 2; j++) {
                ApiKey apiKey;
                if (j == 0) {
                    // Verify the record can be read by authentication ID.
                    apiKey = tableAuthIdentityManagerDAO.getIdentityByAuthenticationId(key);
                } else {
                    // Verify that a lookup by ID works
                    apiKey = tableAuthIdentityManagerDAO.getIdentity(id);
                }
                assertNotNull(apiKey);
                assertEquals(apiKey.getId(), id);
                assertEquals(apiKey.getOwner(), "someone");
                assertEquals(apiKey.getDescription(), "something");
                assertEquals(apiKey.getRoles(), ImmutableList.of("role1", "role2"));
            }
        }
    }
}
