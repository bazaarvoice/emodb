package test.blackbox.web;

import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.queue.api.AuthDedupQueueService;
import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Set;

/**
 * Verifies access to SOA endpoints, REST resources and Tasks included with STANDARD_MAIN.
 */
public class MainRoleInclusionTest extends BaseRoleRestHelper {

    AuthDataStore _dataStore;
    AuthDatabus _databus;
    AuthQueueService _queueService;
    AuthDedupQueueService _dedupQueueService;
    String _apiKey = "anonymous";


    public MainRoleInclusionTest() {
        super ("/config-main-dc1.yaml");
    }

    @BeforeTest
    public void setup() throws Exception {
        _dataStore = getDataStoreViaOstrich();
        _databus = getDatabusViaOstrich();
        _queueService = getQueueServiceViaOstrich();
        _dedupQueueService = getDedupQueueServiceViaOstrich();
    }

    @AfterTest
    public void tearDown () throws Exception {
        close();
    }

    @Test
    public void testDataStoreSOAAccessible () {
        Set<String> configured = _config.getDataStoreConfiguration().getValidTablePlacements();
        Collection<String> present = _dataStore.getTablePlacements(_apiKey);
        configured.removeAll(present);
        Assert.assertEquals(0, configured.size());
    }

    @Test
    public void testDataStoreRESTAccessible () throws Exception {
        super.testDataStoreRESTAccessible();
    }

    @Test
    public void testDatabusSOAAccessible () {
        Assert.assertNotNull(_databus.listSubscriptions(_apiKey, null, 1000));
    }

    @Test
    public void testDatabusRESTAccessible () throws Exception {
        super.testDatabusRESTAccessible();
    }

    @Test
    public void testQueueServiceSOAAccessible () {
        Assert.assertEquals(_queueService.getClaimCount(_apiKey, "test"), 0);
    }

    @Test
    public void testQueueServiceRESTAccessible () throws Exception {
        super.testQueueServiceRESTAccessible();
    }

    @Test
    public void testDedupQueueServiceSOAAccessible () {
        Assert.assertEquals(_dedupQueueService.getMessageCount(_apiKey, "test"), 0);
    }

    @Test
    public void testDedupQueueServiceRESTAccessible () throws Exception {
        super.testDedupQueueServiceRESTAccessible();
    }

    @Test
    public void testDropwizardInvalidationTaskAccessible () throws Exception {
        super.testDropwizardInvalidationTaskAccessible();
    }

    @Test
    public void testLeaderServiceTaskAccessible () throws Exception {
        super.testLeaderServiceTaskAccessible();
    }

    @Test
    public void testThrottleControlTaskAccessible () throws Exception {
        super.testThrottleControlTaskAccessible();
    }

    @Test
    public void testHintsConsistencyTimeTaskAccessible () throws Exception {
        super.testHintsConsistencyTimeTaskAccessible();
    }

    @Test
    public void testMinLagDurationTaskAccessible () throws Exception {
        super.testMinLagDurationTaskAccessible();
    }

    @Test
    public void testSorMoveTableTaskAccessible () throws Exception {
        super.testSorMoveTableTaskAccessible();
    }

    @Test
    public void testSorTableChangesEnabledTaskAccessible () throws Exception {
        super.testSorTableChangesEnabledTaskAccessible();
    }

    @Test
    public void testRowKeyTaskAccessible () throws Exception {
        super.testRowKeyTaskAccessible();
    }

    @Test
    public void testBlobMoveTableTaskAccessible () throws Exception {
        super.testBlobMoveTableTaskAccessible();
    }

    @Test
    public void testBlobTableChangesEnabledTaskAccessible () throws Exception {
        super.testBlobTableChangesEnabledTaskAccessible();
    }

    @Test
    public void testMaintenanceRateLimitTaskAccessible () throws Exception {
        super.testMaintenanceRateLimitTaskAccessible();
    }

    @Test
    public void testReplicationEnabledTaskAccessible () throws Exception {
        super.testReplicationEnabledTaskAccessible();
    }

    @Test
    public void testDedupMigrationTaskAccessible () throws Exception {
        super.testDedupMigrationTaskAccessible();
    }

    @Test
    public void testClaimCountTaskAccessible() throws Exception {
        super.testClaimCountTaskAccessible();
    }

    @Test
    public void testDedupQueueTaskAccessible() throws Exception {
        super.testDedupQueueTaskAccessible();
    }

    @Test
    public void testControlJobServiceTaskAccessible() throws Exception {
        super.testLeaderServiceTaskAccessible();
    }
}
