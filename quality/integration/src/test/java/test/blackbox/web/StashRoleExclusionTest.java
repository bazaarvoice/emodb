package test.blackbox.web;

import javax.ws.rs.WebApplicationException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

/**
 *  Verifies no access to SOA endpoints, REST resources and Tasks excluded from SCANNER.
 */
public class StashRoleExclusionTest extends BaseRoleRestHelper {

    // configuration points directly to stash role server which should fail any non-stash role services
    StashRoleExclusionTest() {
        super ("/config-stash-role.yaml");
    }

    @AfterTest
    public void tearDown () throws Exception {
        close();
    }

    @Test (expectedExceptions = com.bazaarvoice.emodb.client.EmoClientException.class)
    public void testDataStoreNotAccessible () throws Exception  {
        getDataStoreViaFixedHost().getTablePlacements("anonymous");
    }

    @Test (expectedExceptions = com.bazaarvoice.emodb.client.EmoClientException.class)
    public void testDatabusNotAccessible () throws Exception {
        getDatabusViaFixedHost().listSubscriptions("anonymous", null, 1000);
    }

    @Test (expectedExceptions = com.bazaarvoice.emodb.client.EmoClientException.class)
    public void testQueueServiceNotAccessible () throws Exception {
        getQueueServiceViaFixedHost().getMessageCount("anonymous", "test");
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testDataStoreRESTAccessible () throws Exception {
        super.testDataStoreRESTAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testDatabusRESTAccessible () throws Exception {
        super.testDatabusRESTAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testQueueServiceRESTAccessible () throws Exception {
        super.testQueueServiceRESTAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testDedupQueueServiceRESTAccessible () throws Exception {
        super.testDedupQueueServiceRESTAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testHintsConsistencyTimeTaskAccessible () throws Exception {
        super.testHintsConsistencyTimeTaskAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testMinLagDurationTaskAccessible () throws Exception {
        super.testMinLagDurationTaskAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testSorMoveTableTaskAccessible () throws Exception {
        super.testSorMoveTableTaskAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testSorTableChangesEnabledTaskAccessible () throws Exception {
        super.testSorTableChangesEnabledTaskAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testBlobMoveTableTaskAccessible () throws Exception {
        super.testBlobMoveTableTaskAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testBlobTableChangesEnabledTaskAccessible () throws Exception {
        super.testBlobTableChangesEnabledTaskAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testMaintenanceRateLimitTaskAccessible () throws Exception {
        super.testMaintenanceRateLimitTaskAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testReplicationEnabledTaskAccessible () throws Exception {
        super.testReplicationEnabledTaskAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testDedupMigrationTaskAccessible () throws Exception {
        super.testDedupMigrationTaskAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testClaimCountTaskAccessible() throws Exception {
        super.testClaimCountTaskAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testDedupQueueTaskAccessible() throws Exception {
        super.testDedupQueueTaskAccessible();
    }

    @Test (expectedExceptions = com.bazaarvoice.emodb.client.EmoClientException.class)
    public void testBlobStoreNotAccessible () throws Exception {
        getBlobStoreViaFixedHost().getTablePlacements("anonymous");
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testDropwizardInvalidationTaskNotAccessible () throws Exception {
        super.testDropwizardInvalidationTaskAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testThrottleControlTaskAccessible () throws Exception {
        super.testThrottleControlTaskAccessible();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testRowKeyTaskAccessible () throws Exception {
        super.testRowKeyTaskAccessible();
    }
}
