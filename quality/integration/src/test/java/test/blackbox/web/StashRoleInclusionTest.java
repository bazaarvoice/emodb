package test.blackbox.web;

import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;

import static java.lang.String.format;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * Verifies access to SOA endpoints, REST resources and Tasks included with SCANNER.
 */
public class    StashRoleInclusionTest extends BaseRoleRestHelper {

    AuthDataStore _dataStore;

    StashRoleInclusionTest() {
        super("/config-stash-role.yaml");
    }

    @BeforeTest
    public void setup() throws Exception {
        _dataStore = getDataStoreViaOstrich();
    }

    @AfterTest
    public void tearDown () throws Exception {
        close();
    }

    @Test
    public void testLeaderServiceTaskAccessible () throws Exception {
        super.testLeaderServiceTaskAccessible();
    }

    @Test
    public void testScannerIsNotRegisteredWithZookeeper() {
        try (CuratorFramework curator = _config.getZooKeeperConfiguration().newCurator()) {
            curator.start();

            // Make sure that scanner port is registered in the zookeeper cache registry
            String matchingString = format(":%d", getServiceBasePort());
            assertNotNull(matchFound(curator, "/ostrich/local_default-emodb-stash-1", matchingString, null),
                    "Scanner registered not zookeeper");
            // Make sure scanner is not registered for any non-Stash EmoDB zk managed services.
            // Scanner does leader election, so we make sure we exclude the leader zk path
            String matchingPath;
            assertNull((matchingPath = matchFound(curator, "/applications/emodb/local_default", matchingString,
                    "/applications/emodb/local_default/scanner/leader")), format("Scanner registered in zookeeper at: %s", matchingPath));
        }
    }

    @Test
    public void testInvalidationCacheDoesNotInvokeStashRoles() {
        // Create a table in data store that causes invalidation to happen.
        // If its successful, we know Stash isn't being invoked, since that would fail
        _dataStore.createTable("anonymous", "testnewtable", new TableOptionsBuilder().setPlacement("catalog_global:cat").build(),
                ImmutableMap.of("test", "value"), new AuditBuilder().setHost("localhost").setComment("testStashInvalidation").build());
    }

    @Test
    public void testScanUploadResourceAccessible()
            throws Exception {
        String randomName = "test" + Integer.toString(new Random().nextInt());
        super.httpPost(ImmutableMap.<String, Object>of("placement", "catalog_global:cat", "dest", "null"),
                false,
                "stash", "1", "job", randomName);
    }

    private String matchFound(CuratorFramework curator, String path, String matchingString, String exclusion) {
        if (path.equals(exclusion)) {
            // Move on
            return null;
        }
        try {
            List<String> children = curator.getChildren().forPath(path);
            for(String child : children) {
                String match;
                if ((match = matchFound(curator, ZKPaths.makePath(path, child), matchingString, exclusion)) != null) {
                    return match;
                }
            }
            // Only check the leaf nodes
            byte[] data = curator.getData().forPath(path);
            if (data != null) {
                String value = new String(data, Charsets.UTF_8);
                if (value.contains(matchingString)) {
                    return path + ": " + value;
                }
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return null;
    }
}
