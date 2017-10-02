package com.bazaarvoice.emodb.common.cassandra;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import io.dropwizard.lifecycle.Managed;
import org.apache.cassandra.dht.IPartitioner;
import org.slf4j.LoggerFactory;

import java.util.List;

import static java.lang.String.format;

/**
 * Dropwizard managed connection pool for a Cassandra keyspace.
 */
public class CassandraKeyspace implements Managed {
    private final String _keyspaceName;
    private final CassandraCluster<Keyspace> _astyanaxCluster;
    private final CassandraCluster<Session> _cqlCluster;
    private Keyspace _astyanaxKeyspace;
    private CassandraReplication _replication;
    private ConsistencyTopologyAdapter _topologyAdapter;
    private Session _cqlSession;

    public CassandraKeyspace(LifeCycleRegistry lifeCycle, String keyspaceName,
                             CassandraCluster<Keyspace> astyanaxCluster,
                             CassandraCluster<Session> cqlCluster) {
        _keyspaceName = keyspaceName;
        _astyanaxCluster = astyanaxCluster;
        _cqlCluster = cqlCluster;
        lifeCycle.manage(this);
    }

    public void start() throws Exception {
        // Astyanax
        _astyanaxKeyspace = _astyanaxCluster.connect(_keyspaceName);
        _replication = new CassandraReplication(_astyanaxKeyspace.describeKeyspace());
        _topologyAdapter = new ConsistencyTopologyAdapter(_replication);

        // CQL driver
        _cqlSession = _cqlCluster.connect(_keyspaceName);

        errorIfPartitionerMisconfigured();
    }

    public void stop() throws Exception {
        if (_cqlSession != null) {
            _cqlSession.close();
        }
    }

    public String getName() {
        return _keyspaceName;
    }

    public Keyspace getAstyanaxKeyspace() {
        return _astyanaxKeyspace;
    }

    public Session getCqlSession() {
        return _cqlSession;
    }

    public KeyspaceMetadata getKeyspaceMetadata() {
        return _cqlSession.getCluster().getMetadata().getKeyspace(getName());
    }

    public String getClusterName() {
        return _cqlCluster.getClusterName();
    }

    public String getDataCenter() {
        return _cqlCluster.getDataCenter();
    }

    /**
     * Prepare a batch mutation object. It is possible to create multiple batch
     * mutations and later merge them into a single mutation by calling
     * mergeShallow on a batch mutation object.
     */
    public MutationBatch prepareMutationBatch(ConsistencyLevel consistency) {
        return _astyanaxKeyspace.prepareMutationBatch().setConsistencyLevel(clamp(consistency));
    }

    /**
     * Starting point for constructing a query. From the column family the
     * client can perform all 4 types of queries: get column, get key slice, get
     * key range and and index query.
     */
    public <K, C> ColumnFamilyQuery<K, C> prepareQuery(ColumnFamily<K, C> cf, ConsistencyLevel consistency) {
        return _astyanaxKeyspace.prepareQuery(cf).setConsistencyLevel(clamp(consistency));
    }

    /**
     * Mutation for a single column.
     */
    public <K, C> ColumnMutation prepareColumnMutation(ColumnFamily<K, C> cf, K rowKey, C column, ConsistencyLevel consistency) {
        return _astyanaxKeyspace.prepareColumnMutation(cf, rowKey, column).setConsistencyLevel(clamp(consistency));
    }

    /**
     * Downgrades the specified consistency level to be compatible with the Cassandra ring's actual topology.
     * Useful for writing code that runs against rings that vary between single-node testing servers and multi-
     * data center, highly replicated network topologies.
     */
    public ConsistencyLevel clamp(ConsistencyLevel consistencyLevel) {
        return _topologyAdapter.clamp(consistencyLevel);
    }

    private void errorIfPartitionerMisconfigured() {
        String cassandraPartitioner = _cqlSession.getCluster().getMetadata().getPartitioner();
        List<CassandraPartitioner> compatible = Lists.newArrayList();
        for (CassandraPartitioner partitioner : CassandraPartitioner.values()) {
            if (partitioner.matches(cassandraPartitioner)) {
                return;
            }
            compatible.add(partitioner);
        }
        throw new IllegalStateException(format(
                "Cassandra keyspace '%s' uses the %s.  The application connection pool 'partitioner' setting must be set to %s.",
                getName(), cassandraPartitioner.substring(cassandraPartitioner.lastIndexOf('.') + 1),
                Joiner.on(" or ").join(Collections2.transform(compatible, new Function<CassandraPartitioner, Object>() {
            @Override
            public Object apply(CassandraPartitioner partitioner) {
                return "'" + partitioner.name().toLowerCase() + "'";
            }
        }))));
    }

    public void errorIfPartitionerMismatch(Class<? extends IPartitioner> expectedPartitioner) {
        String mismatchedPartitioner = getMismatchedPartitioner(expectedPartitioner);
        if (mismatchedPartitioner != null) {
            throw new IllegalStateException(format(
                    "Cassandra keyspace '%s' must be configured with the %s.  It currently uses %s.",
                    getName(), expectedPartitioner.getSimpleName(), mismatchedPartitioner));
        }
    }

    public void warnIfPartitionerMismatch(Class<? extends IPartitioner> expectedPartitioner) {
        String mismatchedPartitioner = getMismatchedPartitioner(expectedPartitioner);
        if (mismatchedPartitioner != null) {
            LoggerFactory.getLogger(CassandraKeyspace.class).warn(
                    "Cassandra keyspace '{}' would perform better if it was configured with the {}.  It currently uses the {}.",
                    getName(), expectedPartitioner.getSimpleName(), mismatchedPartitioner);
        }
    }

    /**
     * Returns the actual partitioner in use by Cassandra if it does not match the expected partitioner, null if it matches.
     */
    private String getMismatchedPartitioner(Class<? extends IPartitioner> expectedPartitioner) {
        String partitioner = null;
        try {
            partitioner = _astyanaxKeyspace.describePartitioner();
            boolean matches = CassandraPartitioner.fromClass(partitioner).matches(expectedPartitioner.getName());
            if (matches) {
                return null;
            } else {
                return partitioner;
            }
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        } catch (IllegalArgumentException e) {
            // Only thrown if the partitioner doesn't match any compatible partitioner, so by definition it is mismatched.
            return partitioner;
        }

    }
}
