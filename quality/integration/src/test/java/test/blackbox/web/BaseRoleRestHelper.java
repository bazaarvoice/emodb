package test.blackbox.web;

import com.google.common.collect.ImmutableMap;
import org.testng.Assert;

/**
 * Helper defines REST entry point smoke test for EmoDB services
 */
public class BaseRoleRestHelper extends BaseRoleTaskHelper {

    BaseRoleRestHelper (String configFile) {
        super (configFile);
    }

    protected void testDataStoreRESTAccessible () throws Exception {
        Assert.assertNotNull(httpGetServicePortAsList(ImmutableMap.<String, Object>of(), "sor", "1", "_tableplacement"));
    }

    protected void testBlobStoreRESTAccessible () throws Exception {
        Assert.assertNotNull(httpGetServicePortAsList(ImmutableMap.<String, Object>of(), "blob", "1", "_tableplacement"));
    }

    protected void testDatabusRESTAccessible () throws Exception {
        Assert.assertNotNull(httpGetServicePortAsList(ImmutableMap.<String, Object>of(), "bus", "1", "test-subscription", "peek"));
    }

    protected void testQueueServiceRESTAccessible () throws Exception {
        Assert.assertNotNull(httpGetServicePortAsList(ImmutableMap.<String, Object>of(), "queue", "1", "test-queue", "peek"));
    }

    protected void testDedupQueueServiceRESTAccessible () throws Exception {
        Assert.assertNotNull(httpGetServicePortAsList(ImmutableMap.<String, Object>of(), "dedupq", "1", "test-queue", "peek"));
    }

}
