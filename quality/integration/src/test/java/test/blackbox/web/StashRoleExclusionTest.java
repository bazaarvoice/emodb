package test.blackbox.web;

import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

/**
 *  Verifies no access to SOA endpoints, REST resources and Tasks excluded from STASH.
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

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testDataStoreRESTAccessible () throws Exception {
        super.testDataStoreRESTAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testDatabusRESTAccessible () throws Exception {
        super.testDatabusRESTAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testQueueServiceRESTAccessible () throws Exception {
        super.testQueueServiceRESTAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testDedupQueueServiceRESTAccessible () throws Exception {
        super.testDedupQueueServiceRESTAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testHintsConsistencyTimeTaskAccessible () throws Exception {
        super.testHintsConsistencyTimeTaskAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testMinLagDurationTaskAccessible () throws Exception {
        super.testMinLagDurationTaskAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testSorMoveTableTaskAccessible () throws Exception {
        super.testSorMoveTableTaskAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testSorTableChangesEnabledTaskAccessible () throws Exception {
        super.testSorTableChangesEnabledTaskAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testBlobMoveTableTaskAccessible () throws Exception {
        super.testBlobMoveTableTaskAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testBlobTableChangesEnabledTaskAccessible () throws Exception {
        super.testBlobTableChangesEnabledTaskAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testMaintenanceRateLimitTaskAccessible () throws Exception {
        super.testMaintenanceRateLimitTaskAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testReplicationEnabledTaskAccessible () throws Exception {
        super.testReplicationEnabledTaskAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testDedupMigrationTaskAccessible () throws Exception {
        super.testDedupMigrationTaskAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testClaimCountTaskAccessible() throws Exception {
        super.testClaimCountTaskAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testDedupQueueTaskAccessible() throws Exception {
        super.testDedupQueueTaskAccessible();
    }

    @Test (expectedExceptions = com.bazaarvoice.emodb.client.EmoClientException.class)
    public void testBlobStoreNotAccessible () throws Exception {
        getBlobStoreViaFixedHost().getTablePlacements("anonymous");
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testDropwizardInvalidationTaskNotAccessible () throws Exception {
        super.testDropwizardInvalidationTaskAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testThrottleControlTaskAccessible () throws Exception {
        super.testThrottleControlTaskAccessible();
    }

    @Test (expectedExceptions = com.sun.jersey.api.client.UniformInterfaceException.class)
    public void testRowKeyTaskAccessible () throws Exception {
        super.testRowKeyTaskAccessible();
    }
}
