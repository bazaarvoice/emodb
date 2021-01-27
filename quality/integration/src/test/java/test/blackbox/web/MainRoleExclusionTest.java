package test.blackbox.web;

import org.testng.annotations.Test;

/**
 *  Verifies exclusion of SOA endpoints, REST resources and Tasks excluded from STANDARD_MAIN.
 */
public class MainRoleExclusionTest extends BaseRoleConnectHelper {

    // configuration points directly to main role server (ELB) which should fail any non- main role services
    MainRoleExclusionTest() {
        super ("/config-main-dc1.yaml"); // mis-configured main role
    }

    @Test (expectedExceptions = com.bazaarvoice.emodb.client.EmoClientException.class)
    public void testBlobStoreNotAccessible () throws Exception {
        getBlobStoreViaFixedHost().getTablePlacements("anonymous");
    }
}
