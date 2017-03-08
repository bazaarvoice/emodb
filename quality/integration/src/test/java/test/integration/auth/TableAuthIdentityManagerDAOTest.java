package test.integration.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
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

import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

public class TableAuthIdentityManagerDAOTest {

    /**
     * There are two tables which store identities in TableAuthIdentityManagerDAO: One table keyed by a hash of the
     * API key, and an index table ID'd by the internal ID which contains the API key hash.  This second table is used
     * to look up API keys by internal ID.  It should be rare, but it is possible for an API key record to exist
     * without a corresponding internal ID.  One possible way for this to happen is grandfathered in API keys
     * created before the introduction of internal IDs.  TableAuthIdentityManagerDAO should rebuild the index
     * when there is a missing or incorrect index record.  This test verifies that works as expected.
     */
    @Test
    public void testRebuildInternalIdIndex() {
        DataStore dataStore = new InMemoryDataStore(new MetricRegistry());
        TableAuthIdentityManagerDAO<ApiKey> tableAuthIdentityManagerDAO = new TableAuthIdentityManagerDAO<>(
                ApiKey.class, dataStore, "__auth:keys", "__auth:internal_ids", "app_global:sys", Hashing.sha256());

        ApiKey apiKey = new ApiKey("testkey", "id0", ImmutableSet.of("role1", "role2"));
        apiKey.setOwner("testowner");
        tableAuthIdentityManagerDAO.updateIdentity(apiKey);

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

        // Verify that a lookup by internal ID works
        Set<String> roles = tableAuthIdentityManagerDAO.getRolesByInternalId("id0");
        assertEquals(roles, ImmutableSet.of("role1", "role2"));

        // Verify that the index record is re-created
        indexMap = dataStore.get("__auth:internal_ids", "id0");
        assertFalse(Intrinsic.isDeleted(indexMap));
        assertEquals(indexMap.get("hashedId"), keyTableId);
    }

    @Test
    public void testGrandfatheredInInternalId() {
        DataStore dataStore = new InMemoryDataStore(new MetricRegistry());
        TableAuthIdentityManagerDAO<ApiKey> tableAuthIdentityManagerDAO = new TableAuthIdentityManagerDAO<>(
                ApiKey.class, dataStore, "__auth:keys", "__auth:internal_ids", "app_global:sys", Hashing.sha256());

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

        // Verify the record can be read by ID.  The key's internal ID will be the hashed ID.
        ApiKey apiKey = tableAuthIdentityManagerDAO.getIdentity(id);
        assertNotNull(apiKey);
        assertEquals(apiKey.getId(), id);
        assertEquals(apiKey.getInternalId(), hash);
        assertEquals(apiKey.getOwner(), "someone");
        assertEquals(apiKey.getDescription(), "something");
        assertEquals(apiKey.getRoles(), ImmutableList.of("role1", "role2"));

        // Verify that a lookup by internal ID works
        Set<String> roles = tableAuthIdentityManagerDAO.getRolesByInternalId(hash);
        assertEquals(roles, ImmutableSet.of("role1", "role2"));

        // Verify that the index record was created with the hashed ID as the internal ID
        Map<String, Object> indexMap = dataStore.get("__auth:internal_ids", hash);
        assertFalse(Intrinsic.isDeleted(indexMap));
        assertEquals(indexMap.get("hashedId"), hash);

        // Verify lookup by internal ID still works with the index record in place
        roles = tableAuthIdentityManagerDAO.getRolesByInternalId(hash);
        assertEquals(roles, ImmutableSet.of("role1", "role2"));
    }
}
