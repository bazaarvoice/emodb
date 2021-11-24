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
import com.datastax.driver.core.VersionNumber;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.lang.String.format;

public class ClusterHintsPoller {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterHintsPoller.class);

    /**
     * In Cassandra 3.x, hints are no longer stored in system.hints table.
     * They are stored in flat files, bypassing the storage engine altogether.
     * More info on why this change was made can be found here" https://www.datastax.com/dev/blog/whats-coming-to-cassandra-in-3-0-improved-hint-storage-and-delivery
     */
    private static final VersionNumber CASSANDRA_VERSION_3_0_0 = VersionNumber.parse("3.0.0");
    private static final int DEFAULT_CASSANDRA_JMX_PORT = 7199;

    @VisibleForTesting
    protected static final String DISTINCT_TARGET_IDS_QUERY = "SELECT DISTINCT target_id FROM hints";
    @VisibleForTesting
    protected static final String OLDEST_HINT_QUERY_FORMAT = "SELECT hint_id FROM hints WHERE target_id IN (%s) ORDER BY hint_id ASC LIMIT 1";


    /**
     * @return HintsPollerResult that has results from the HintsPoller for the entire ring
     */
    public HintsPollerResult getOldestHintsInfo(Session session) {
        HintsPollerResult hintsPollerResult = new HintsPollerResult();
        Cluster cluster = session.getCluster();
        String clusterName = cluster.getClusterName();
        LOGGER.debug("Connected to cluster: '{}'\n", clusterName);
        Metadata metadata = cluster.getMetadata();

        // Support for the min() aggregate function was only added from CQL 3.3.0 and C* 2.0.17 uses 3.1.7:
        // String hintIdsQuery = "SELECT min(hint_id) AS old_hint_id FROM hints";

        // ORDER BY is only supported when the partition key is restricted by an EQ or an IN. So, we cannot run the below query.
        // String hintIdsQuery = "SELECT hint_id FROM hints ORDER BY hint_id ASC LIMIT 1";

        for (Host host : metadata.getAllHosts()) {
            LOGGER.debug("Looking for hints on host: '{}'\n", host.getAddress());

            VersionNumber cassandraVersion = host.getCassandraVersion();
            if (CASSANDRA_VERSION_3_0_0.compareTo(cassandraVersion) <= 0) {
                try (JmxClient jmxClient = new JmxClient(host.getAddress().getHostName(), DEFAULT_CASSANDRA_JMX_PORT)) {
                    // TODO rewrite to use org.apache.cassandra.metrics:type=HintsService during C* 3.11 migration
                    ObjectName name = new ObjectName("org.apache.cassandra.metrics:type=Storage,name=TotalHintsInProgress");
                    long hintsInProgressCount = (Long) jmxClient.getAttribute(name, "Count");

                    if (hintsInProgressCount > 0) {
                        LOGGER.debug("In progress hints found on host: {}", host.getAddress());
                        hintsPollerResult.setHostWithFailure(host.getAddress());
                    } else {
                        hintsPollerResult.setHintsResult(host.getAddress(), Optional.absent());
                    }
                } catch (IOException | MalformedObjectNameException | AttributeNotFoundException | InstanceNotFoundException | MBeanException | ReflectionException e) {
                    LOGGER.warn("Couldn't fetch Hints JMX metrics on host: '{}'\n{}", host.getAddress(), e.getMessage());
                    // This means we were not able to check the hints on this host - host may be down or a connection problem.
                    // If so, just return null - hints on other hosts doesn't matter.
                    return hintsPollerResult.setHostWithFailure(host.getAddress());
                }
            } else {
                long startTime = System.currentTimeMillis();

                // To bypass the restriction for this version of CQL, we query the targetIds first and pass them in a IN for the hints query.
                // We could also get all the Node names from the cluster's metadata and just pass them as TargetIds for the IN condition in the hints query.
                ResultSet targetIdsResult;
                try {
                    targetIdsResult = session.execute(new SelectedHostStatement(new SimpleStatement(DISTINCT_TARGET_IDS_QUERY), host));
                } catch (NoHostAvailableException ex) {
                    LOGGER.warn("Couldn't run the target Ids query on host: '{}'\n", host.getAddress());
                    // This means we were not able to check the hints on this host - host may be down or a connection problem.
                    // If so, just return null - hints on other hosts doesn't matter.
                    return hintsPollerResult.setHostWithFailure(host.getAddress());
                }
                List<UUID> targetIds = new ArrayList<>();
                for (Row row : targetIdsResult) {
                    targetIds.add(row.getUUID("target_id"));
                }
                // If no targetIDs are found, then it means there are not hints on the node. No need to query for oldest hint.
                if (targetIds.isEmpty()) {
                    LOGGER.debug("Cassandra cluster: '{}', Node: '{}',  NO hints", clusterName, host);
                    hintsPollerResult.setHintsResult(host.getAddress(), Optional.absent());
                    continue;
                }

                String hintIdsQuery = format(OLDEST_HINT_QUERY_FORMAT, Joiner.on(",").join(targetIds));
                ResultSet hintIdsResult;
                try {
                    hintIdsResult = session.execute(new SelectedHostStatement(new SimpleStatement(hintIdsQuery), host));
                } catch (NoHostAvailableException ex) {
                    LOGGER.warn("Couldn't run the hint Ids query on host: '{}'\n", host.getAddress());
                    // This means we were not able check the hints on this host - host may be down or a connection problem.
                    // If so, just return null - hints on other hosts doesn't matter.
                    return hintsPollerResult.setHostWithFailure(host.getAddress());
                }

                Row oldestHintId;
                if ((oldestHintId = hintIdsResult.one()) == null) {
                    // It is possible that by this time all hints were cleared, and we returned nothing
                    LOGGER.debug("Cassandra cluster: '{}', Node: '{}',  NO hints", clusterName, host);
                    hintsPollerResult.setHintsResult(host.getAddress(), Optional.absent());
                    continue;
                }

                UUID oldHintTimeUUID = oldestHintId.getUUID("hint_id");

                long stopTime = System.currentTimeMillis();
                long elapsedTime = stopTime - startTime;
                LOGGER.debug("Time taken to execute query: " + elapsedTime);

                if (oldHintTimeUUID != null) {
                    long timeInMillis = TimeUUIDs.getTimeMillis(oldHintTimeUUID);
                    LOGGER.debug("Cassandra cluster: '{}', Node: '{}', Oldest hint time: '{}'", clusterName, host, timeInMillis);

                    hintsPollerResult.setHintsResult(host.getAddress(), Optional.of(timeInMillis));
                } else {
                    LOGGER.debug("Cassandra cluster: '{}', Node: '{}',  NO hints", clusterName, host);
                    hintsPollerResult.setHintsResult(host.getAddress(), Optional.absent());
                }
            }
        }

        return hintsPollerResult;
    }
}
