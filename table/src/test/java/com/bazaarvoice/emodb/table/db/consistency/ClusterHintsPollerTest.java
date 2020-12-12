package com.bazaarvoice.emodb.table.db.consistency;

import com.bazaarvoice.emodb.common.cassandra.cqldriver.SelectedHostStatement;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import static java.lang.String.format;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ClusterHintsPollerTest {

    @Test
    public void testClusterHintsPollerWhenNodeDown() throws UnknownHostException {
        ClusterHintsPoller clusterHintsPoller = new ClusterHintsPoller();
        Session mockSession = mock(Session.class);
        Cluster mockCluster = mock(Cluster.class);
        Metadata mockMetadata = mock(Metadata.class);
        when(mockCluster.getMetadata()).thenReturn(mockMetadata);
        when(mockCluster.getClusterName()).thenReturn("test-cluster");
        Host node1 = mock(Host.class);
        when(node1.getAddress()).thenReturn(InetAddress.getByName("127.0.0.1"));
        Host node2 = mock(Host.class);
        when(node2.getAddress()).thenReturn(InetAddress.getByName("127.0.0.2"));
        Host node3 = mock(Host.class);
        when(node3.getAddress()).thenReturn(InetAddress.getByName("127.0.0.3"));

        when(mockSession.getCluster()).thenReturn(mockCluster);
        // The first node queried is down
        when(mockSession.execute(any(Statement.class))).thenThrow(new NoHostAvailableException(ImmutableMap.of()));

        when(mockMetadata.getAllHosts()).thenReturn(ImmutableSet.of(node1, node2, node3));
        HintsPollerResult actualResult = clusterHintsPoller.getOldestHintsInfo(mockSession);

        // Make sure HintsPollerResult fails
        assertFalse(actualResult.areAllHostsPolling(), "Result should show hosts failing");
        assertEquals(actualResult.getHostFailure(), ImmutableSet.of(InetAddress.getByName("127.0.0.1")), "Node 1 should return with host failure");
    }

    /**
     * This test mocks 2 nodes that both return two target Id's with hints, and verifies that oldest hint is returned
     */
    @Test
    public void testClusterHintsPollerWithOldestHint() throws Exception {

        // Mocks

        ClusterHintsPoller clusterHintsPoller = new ClusterHintsPoller();
        Session mockSession = mock(Session.class);
        Cluster mockCluster = mock(Cluster.class);
        Metadata mockMetadata = mock(Metadata.class);
        when(mockCluster.getMetadata()).thenReturn(mockMetadata);
        when(mockCluster.getClusterName()).thenReturn("test-cluster");
        Host node1 = mock(Host.class);
        when(node1.getAddress()).thenReturn(InetAddress.getByName("127.0.0.1"));
        Host node2 = mock(Host.class);
        when(node2.getAddress()).thenReturn(InetAddress.getByName("127.0.0.2"));

        when(mockSession.getCluster()).thenReturn(mockCluster);

        // Create target ids with sample timestamps
        UUID targetId1 = TimeUUIDs.newUUID();
        UUID targetId2 = TimeUUIDs.newUUID();
        Row rowTargetIdNode1 = mock(Row.class);
        Row rowTargetIdNode2 = mock(Row.class);
        // Mock both nodes to return two target Id's with hints
        when(rowTargetIdNode1.getUUID("target_id")).thenReturn(targetId1).thenReturn(targetId1);
        when(rowTargetIdNode2.getUUID("target_id")).thenReturn(targetId2).thenReturn(targetId2);

        // Mock node 1 ResultSets
        ResultSet targetIdQueryNode1 = mock(ResultSet.class);
        when(targetIdQueryNode1.iterator()).thenReturn(Lists.newArrayList(rowTargetIdNode1, rowTargetIdNode2).iterator());
        ResultSet hintsQueryNode1 = mock(ResultSet.class);
        Row mockOldestHintIdOnNode1 = mock(Row.class);
        // Oldest hint on the first node is 2 minutes ago
        long oldestHintOnNode1 = Instant.now().minus(Duration.ofMinutes(2)).toEpochMilli();
        when(mockOldestHintIdOnNode1.getUUID("hint_id")).thenReturn(TimeUUIDs.uuidForTimeMillis(oldestHintOnNode1));
        when(hintsQueryNode1.one()).thenReturn(mockOldestHintIdOnNode1);

        // Mock node 2 ResultSets
        ResultSet targetIdQueryNode2 = mock(ResultSet.class);
        when(targetIdQueryNode2.iterator()).thenReturn(Lists.newArrayList(rowTargetIdNode1, rowTargetIdNode2).iterator());
        ResultSet hintsQueryNode2 = mock(ResultSet.class);
        Row mockOldestHintIdOnNode2 = mock(Row.class);
        // Oldest hint on the first node is 3 minutes ago
        long oldestHintOnNode2 = Instant.now().minus(Duration.ofMinutes(3)).toEpochMilli();
        when(mockOldestHintIdOnNode2.getUUID("hint_id")).thenReturn(TimeUUIDs.uuidForTimeMillis(oldestHintOnNode2));
        when(hintsQueryNode2.one()).thenReturn(mockOldestHintIdOnNode2);


        // The following line mocks all the results we will get back from our CQL queries
        doReturn(targetIdQueryNode1).when(mockSession).execute(argThat(getHostStatementMatcher(node1,
                ClusterHintsPoller.DISTINCT_TARGET_IDS_QUERY)));
        doReturn(targetIdQueryNode2).when(mockSession).execute(argThat(getHostStatementMatcher(node2,
                ClusterHintsPoller.DISTINCT_TARGET_IDS_QUERY)));
        String targetIds = Joiner.on(",").join(ImmutableList.of(targetId1, targetId2));
        doReturn(hintsQueryNode1).when(mockSession).execute(argThat(getHostStatementMatcher(node1,
                format(ClusterHintsPoller.OLDEST_HINT_QUERY_FORMAT, targetIds))));
        doReturn(hintsQueryNode2).when(mockSession).execute(argThat(getHostStatementMatcher(node2,
                format(ClusterHintsPoller.OLDEST_HINT_QUERY_FORMAT, targetIds))));

        when(mockMetadata.getAllHosts()).thenReturn(ImmutableSet.of(node1, node2));
        HintsPollerResult actualResult = clusterHintsPoller.getOldestHintsInfo(mockSession);

        // Make sure HintsPollerResult gives us the oldest hint (oldest hint is on node 2)
        assertTrue(actualResult.getOldestHintTimestamp().isPresent(), "Hints are there, but none found.");
        assertEquals((long) actualResult.getOldestHintTimestamp().get(), oldestHintOnNode2);
        assertTrue(actualResult.areAllHostsPolling(), "All hosts should be polling fine");
        assertEquals(actualResult.getAllPolledHosts(),
                ImmutableSet.of(InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.2")));

        verify(mockSession, times(1)).execute(argThat(getHostStatementMatcher(node1,
                ClusterHintsPoller.DISTINCT_TARGET_IDS_QUERY)));
        verify(mockSession, times(1)).execute(argThat(getHostStatementMatcher(node2,
                ClusterHintsPoller.DISTINCT_TARGET_IDS_QUERY)));
        verify(mockSession, times(1)).execute(argThat(getHostStatementMatcher(node1,
                format(ClusterHintsPoller.OLDEST_HINT_QUERY_FORMAT, targetIds))));
        verify(mockSession, times(1)).execute(argThat(getHostStatementMatcher(node2,
                format(ClusterHintsPoller.OLDEST_HINT_QUERY_FORMAT, targetIds))));
    }

    /**
     * This test verifies when there are no hints on any of the nodes
     */
    @Test
    public void testClusterHintsPollerWhenThereAreNoHints()
            throws Exception {
        // Mocks

        ClusterHintsPoller clusterHintsPoller = new ClusterHintsPoller();
        Session mockSession = mock(Session.class);
        Cluster mockCluster = mock(Cluster.class);
        Metadata mockMetadata = mock(Metadata.class);
        when(mockCluster.getMetadata()).thenReturn(mockMetadata);
        when(mockCluster.getClusterName()).thenReturn("test-cluster");
        Host node1 = mock(Host.class);
        when(node1.getAddress()).thenReturn(InetAddress.getByName("127.0.0.1"));
        Host node2 = mock(Host.class);
        when(node2.getAddress()).thenReturn(InetAddress.getByName("127.0.0.2"));

        when(mockSession.getCluster()).thenReturn(mockCluster);

        // Create target ids with sample timestamps

        // Mock node 1 ResultSets
        ResultSet targetIdQueryNode1 = mock(ResultSet.class);
        when(targetIdQueryNode1.iterator()).thenReturn(Iterators.emptyIterator());

        // Mock node 2 ResultSets
        ResultSet targetIdQueryNode2 = mock(ResultSet.class);
        when(targetIdQueryNode2.iterator()).thenReturn(Iterators.emptyIterator());

        // The following line mocks all the results we will get back from our CQL queries
        doReturn(targetIdQueryNode1).when(mockSession).execute(argThat(getHostStatementMatcher(node1,
                ClusterHintsPoller.DISTINCT_TARGET_IDS_QUERY)));
        doReturn(targetIdQueryNode2).when(mockSession).execute(argThat(getHostStatementMatcher(node2,
                ClusterHintsPoller.DISTINCT_TARGET_IDS_QUERY)));

        when(mockMetadata.getAllHosts()).thenReturn(ImmutableSet.of(node1, node2));
        HintsPollerResult actualResult = clusterHintsPoller.getOldestHintsInfo(mockSession);

        // Make sure HintsPollerResult shows that no hints were found
        assertFalse(actualResult.getOldestHintTimestamp().isPresent(), "No hints should be found");
        assertTrue(actualResult.areAllHostsPolling(), "All hosts should be polling fine");
        assertEquals(actualResult.getAllPolledHosts(),
                ImmutableSet.of(InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.2")));

        verify(mockSession, times(1)).execute(argThat(getHostStatementMatcher(node1,
                ClusterHintsPoller.DISTINCT_TARGET_IDS_QUERY)));
        verify(mockSession, times(1)).execute(argThat(getHostStatementMatcher(node2,
                ClusterHintsPoller.DISTINCT_TARGET_IDS_QUERY)));
    }

    /**
     * This tests a race condition where a node returns a non-empty set of target_ids, but on the subsequent call those hints are cleared
     */
    @Test
    public void testClusterHintsPollerWithNoHintsOnSubsequentCall()
            throws Exception {

        // Mocks

        ClusterHintsPoller clusterHintsPoller = new ClusterHintsPoller();
        Session mockSession = mock(Session.class);
        Cluster mockCluster = mock(Cluster.class);
        Metadata mockMetadata = mock(Metadata.class);
        when(mockCluster.getMetadata()).thenReturn(mockMetadata);
        when(mockCluster.getClusterName()).thenReturn("test-cluster");
        Host node1 = mock(Host.class);
        when(node1.getAddress()).thenReturn(InetAddress.getByName("127.0.0.1"));
        Host node2 = mock(Host.class);
        when(node2.getAddress()).thenReturn(InetAddress.getByName("127.0.0.2"));

        when(mockSession.getCluster()).thenReturn(mockCluster);

        // Create target ids with sample timestamps
        UUID targetId1 = TimeUUIDs.newUUID();
        UUID targetId2 = TimeUUIDs.newUUID();
        Row rowTargetIdNode1 = mock(Row.class);
        Row rowTargetIdNode2 = mock(Row.class);
        // Mock both nodes to return two target Id's with hints
        when(rowTargetIdNode1.getUUID("target_id")).thenReturn(targetId1);
        when(rowTargetIdNode2.getUUID("target_id")).thenReturn(targetId2);

        // Mock node 1 ResultSets
        ResultSet targetIdQueryNode1 = mock(ResultSet.class);
        when(targetIdQueryNode1.iterator()).thenReturn(Iterators.emptyIterator());

        // Mock node 2 ResultSets
        ResultSet targetIdQueryNode2 = mock(ResultSet.class);
        when(targetIdQueryNode2.iterator()).thenReturn(Lists.newArrayList(rowTargetIdNode1, rowTargetIdNode2).iterator());
        ResultSet hintsQueryNode2 = mock(ResultSet.class);
        Row mockOldestHintIdOnNode2 = mock(Row.class);
        // Oldest hint on the first node is 3 minutes ago
        long oldestHintOnNode2 = Instant.now().minus(Duration.ofMinutes(3)).toEpochMilli();
        when(mockOldestHintIdOnNode2.getUUID("hint_id")).thenReturn(TimeUUIDs.uuidForTimeMillis(oldestHintOnNode2));
        // We will explicitly return null here. This simulates where we first query for target Ids and find some, but by
        // the time we query for the oldest hint id for those target ids, we don't find any as they are already gone.
        when(hintsQueryNode2.one()).thenReturn(null);


        // The following line mocks all the results we will get back from our CQL queries
        // Note that this is based on the knowledge that we have about the order of CQL queries made in
        // ClusterHintsPoller.getOldestHintsInfo method.
        // If in future, the way we make CQL queries in the above method is changed, then we would have to rewrite the test.
        doReturn(targetIdQueryNode1).when(mockSession).execute(argThat(getHostStatementMatcher(node1,
                ClusterHintsPoller.DISTINCT_TARGET_IDS_QUERY)));
        doReturn(targetIdQueryNode2).when(mockSession).execute(argThat(getHostStatementMatcher(node2,
                ClusterHintsPoller.DISTINCT_TARGET_IDS_QUERY)));
        String targetIds = Joiner.on(",").join(ImmutableList.of(targetId1, targetId2));
        doReturn(hintsQueryNode2).when(mockSession).execute(argThat(getHostStatementMatcher(node2,
                format(ClusterHintsPoller.OLDEST_HINT_QUERY_FORMAT, targetIds))));

        when(mockMetadata.getAllHosts()).thenReturn(ImmutableSet.of(node1, node2));
        HintsPollerResult actualResult = clusterHintsPoller.getOldestHintsInfo(mockSession);

        // Make sure HintsPollerResult shows that no hints were found
        assertFalse(actualResult.getOldestHintTimestamp().isPresent(), "No hints should be found");
        assertTrue(actualResult.areAllHostsPolling(), "All hosts should be polling fine");
        assertEquals(actualResult.getAllPolledHosts(),
                ImmutableSet.of(InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.2")));

        verify(mockSession, times(1)).execute(argThat(getHostStatementMatcher(node1,
                ClusterHintsPoller.DISTINCT_TARGET_IDS_QUERY)));
        verify(mockSession, times(1)).execute(argThat(getHostStatementMatcher(node2,
                ClusterHintsPoller.DISTINCT_TARGET_IDS_QUERY)));
        verify(mockSession, times(1)).execute(argThat(getHostStatementMatcher(node2,
                format(ClusterHintsPoller.OLDEST_HINT_QUERY_FORMAT, targetIds))));
    }

    private ArgumentMatcher<Statement> getHostStatementMatcher(final Host host, final String query) {
        return new ArgumentMatcher<Statement>() {
            @Override
            public boolean matches(Statement argument) {
                SelectedHostStatement statement = (SelectedHostStatement) argument;

                return ((SimpleStatement) statement.getStatement()).getQueryString().equals(query) &&
                        Objects.equals(statement.getHostCordinator().getAddress(), host.getAddress());
            }

            @Override
            public String toString() {
                return format("query:%s host:%s", query, host.getAddress().toString());
            }
        };
    }
}