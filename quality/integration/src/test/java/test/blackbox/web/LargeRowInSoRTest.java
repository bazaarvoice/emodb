package test.blackbox.web;

import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Collections;

import static java.lang.String.format;
import static org.testng.Assert.fail;

/**
 * This tests CQL vs Astyanax DAO implementations when confronted with a wide row of large deltas.
 * Astyanax fails with a thrift frame size error. CQL driver on the other hand should just be able
 * to stream over deltas iteratively. This is a test of survival not performance. To test, we
 * will limit each delta to a 3 MB size. And store 16 such deltas in a single Emo Row.
 */
public class LargeRowInSoRTest extends BaseRoleConnectHelper {

    LargeRowInSoRTest() {
        super("/config-main-role.yaml");
    }

    /**
     * This test is disabled because it puts all Astyanax candidates in the bad pool that causes subsequent tests to fail.
     * Since primarily, this test proves how CQL DAO does better than Astyanax, we can just leave it disabled.
     */
    @Test(enabled = false)
    public void testWideRowsWithHeavyDeltas()
            throws Exception {
        AuthDataStore dataStore = getDataStoreViaFixedHost();
        // Generate a delta of about 3MB in size
        long maxDeltaSize = 3L * 1024 * 1024; // 3 MB
        StringBuilder sb = new StringBuilder((int) maxDeltaSize);
        for (long i=0; i < maxDeltaSize; i++) {
            sb.append("a");
        }
        // Create a test table
        String table = "test-wide-rows";
        String key = "wide-row";
        Audit audit = new AuditBuilder().setProgram("test").setLocalHost().setComment("test").build();
        dataStore.createTable("anonymous", table, new TableOptionsBuilder().setPlacement("catalog_global:cat").build(),
                Collections.<String, Object>emptyMap(), audit);

        // Create 16 deltas of 3 MB each
        for (int i = 0; i < 16; i++) {
            dataStore.update("anonymous", table, key, TimeUUIDs.newUUID(),
                    Deltas.fromString(format("{..,\"bigtextfield\":\"%s\"}", sb.toString())), audit, WriteConsistency.STRONG);
        }

        // Fetch the row using default CQL - should be successful given unlimited timeout
        dataStore.get("anonymous", table, key, ReadConsistency.STRONG);

        // Toggle driver to use Astyanax
        httpPost(ImmutableMap.<String, Object>of("driver", "astyanax"), "tasks", "cql-toggle");

        // Fetch it again. This time using Astyanax and we should get an error.
        try {
            dataStore.get("anonymous", table, key, ReadConsistency.STRONG);
            fail();
        } catch (Exception ex) {
            System.out.println("As expected, astyanax DAO couldn't get the document. Exception Message: " + ex.getMessage());
        }

        // Toggle it back to CQL
        httpPost(ImmutableMap.<String, Object>of("driver", "cql"), "tasks", "cql-toggle");
    }
}
