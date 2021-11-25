package test.blackbox.web;

import com.bazaarvoice.emodb.common.cassandra.cqldriver.SelectedHostLoadBalancingPolicy;
import com.bazaarvoice.emodb.common.cassandra.cqldriver.SelectedHostStatement;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.table.db.consistency.ClusterHintsPoller;
import com.bazaarvoice.emodb.table.db.consistency.HintsPollerResult;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.collect.Lists;
import org.junit.Ignore;
import org.junit.Test;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class HintsPollerTest extends BaseRoleConnectHelper {

    public static String TEST_TARGET_ID = UUID.randomUUID().toString();

    public static List<Host> ALL_SELECTED_HOSTS = new ArrayList<>();

    public HintsPollerTest() {
        super("/config-all-role.yaml");
    }

    @Test
    public void oldestHintIsPickedFromOneNode() {

        long hintTimestamp = System.currentTimeMillis();

        Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withPort(9164)
                .withClusterName("Test Cluster")
                .build();

        Session clusterSession = cluster.connect("system");

        // Insert two hints with different timestamps on the same node.
        clusterSession.execute(getInsertHintsQuery(hintTimestamp));
        clusterSession.execute(getInsertHintsQuery(hintTimestamp + 10000));

        // Get oldestHint
        ClusterHintsPoller clusterHintsPoller = new ClusterHintsPoller();
        HintsPollerResult oldestHintsInfo = clusterHintsPoller.getOldestHintsInfo(clusterSession);
        Assert.assertNotNull(oldestHintsInfo);

        // Since we know there should be only one entry
        long retrievedHintTimeStamp = oldestHintsInfo.getOldestHintTimestamp().or(Long.MAX_VALUE);

        Assert.assertEquals(retrievedHintTimeStamp, hintTimestamp);

        cluster.close();
        clusterSession.close();
    }

    @Ignore
    @Test
    public void queryIsExecutedOnAllClusterNodesAndAlsoTheOldestHintIsPicked() {

        // ***** TODO: Get a cluster with 2 nodes. (RUN WITH PERFTEST LATER.....) ******
        // Ignoring this test for now

        // Insert hints on all the cluster nodes.
        Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withPort(9164)
                .withLoadBalancingPolicy(new SelectedHostLoadBalancingPolicyForTest())
                .build();
        Metadata metadata = cluster.getMetadata();
        Session clusterSession = cluster.connect();

        long hintTimestamp = System.currentTimeMillis();
        for (Host host : metadata.getAllHosts()) {
            SelectedHostStatement selectedHostStatement = new SelectedHostStatement(new SimpleStatement(getInsertHintsQuery(hintTimestamp)), host);
            clusterSession.execute(selectedHostStatement);
            hintTimestamp = hintTimestamp + 10000;
        }

        // Now check if the query ran on EVERY node of the cluster.
        Assert.assertEquals(ALL_SELECTED_HOSTS.size(), 2);

        // Get the oldest hint Timestamp of the cluster
        ClusterHintsPoller clusterHintsPoller = new ClusterHintsPoller();
        HintsPollerResult oldestHintsInfo = clusterHintsPoller.getOldestHintsInfo(clusterSession);

        // Note: ?? - This will make the test fail even if one node is down or a connection problem with just one node.
        Assert.assertNotNull(oldestHintsInfo);
        Assert.assertEquals(oldestHintsInfo.getAllPolledHosts().size(), 2);

        long retrievedHintTimeStamp = oldestHintsInfo.getOldestHintTimestamp().or(Long.MAX_VALUE);

        Assert.assertEquals(retrievedHintTimeStamp, hintTimestamp);

        cluster.close();
        clusterSession.close();
    }

    private String getInsertHintsQuery(long hintTimestamp) {

        // bigintAsBlob is not supported with Cassandra 2.0 - it works with Cassandra 2.2.4.
//        return "INSERT INTO system.hints (target_id, hint_id, message_version, mutation) " +
//                "VALUES (" + TEST_TARGET_ID + ", " + TimeUUIDs.uuidForTimeMillis(hintTimestamp) + ", 1, system.bigintAsBlob(3))";

        return "INSERT INTO system.hints (target_id, hint_id, message_version, mutation) " +
                "VALUES (" + TEST_TARGET_ID + ", " + TimeUUIDs.uuidForTimeMillis(hintTimestamp) + ", 1, 0x0000000000000003)";
    }

    private static class SelectedHostLoadBalancingPolicyForTest extends SelectedHostLoadBalancingPolicy {
        // this method is called for every query to choose the hosts that the query should run on.
        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            Iterator<Host> hostIterator = super.newQueryPlan(loggedKeyspace, statement);
            List<Host> selectedHosts = Lists.newArrayList(hostIterator);
            // Only one host is to be selected to run the query
            Assert.assertEquals(selectedHosts.size(), 1, "There should be only one host selected");
            // Add the host for the later check
            ALL_SELECTED_HOSTS.addAll(selectedHosts);
            return hostIterator;
        }
    }

}