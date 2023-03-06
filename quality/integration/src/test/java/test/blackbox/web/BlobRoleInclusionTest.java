package test.blackbox.web;

import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Set;

/**
 * Verifies access to SOA endpoints, REST resources and Tasks included with STANDARD_BLOB.
 */
public class BlobRoleInclusionTest extends BaseRoleRestHelper {

    AuthBlobStore _blobStore;

    BlobRoleInclusionTest() {
        super ("/config-all-role.yaml");
    }

    @BeforeTest
    public void setup() throws Exception {
        _blobStore = getBlobStoreViaOstrich();
    }

    @AfterTest
    public void tearDown () throws Exception {
        close();
    }

//    @Test
    public void testBlobStoreSOAAccessible () { //FIXME
        Set<String> configured = _config.getBlobStoreConfiguration().getValidTablePlacements();
        Collection<String> present = _blobStore.getTablePlacements("anonymous");
        configured.removeAll(present);
        Assert.assertEquals(0, configured.size());
    }

    @Test
    public void testBlobStoreRESTAccessible () throws Exception {
        super.testBlobStoreRESTAccessible();
    }

    @Test
    public void testDropwizardInvalidationTaskAccessible () throws Exception {
        super.testDropwizardInvalidationTaskAccessible();
    }

    @Test
    public void testThrottleControlTaskAccessible () throws Exception {
       super.testThrottleControlTaskAccessible();
    }

    @Test
    public void testRowKeyTaskAccessible () throws Exception {
        super.testRowKeyTaskAccessible();
    }

    @Test
    public void testLeaderServiceTaskAccessible () throws Exception {
        super.testLeaderServiceTaskAccessible();
    }

}
