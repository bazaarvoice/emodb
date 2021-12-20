package com.bazaarvoice.emodb.event.db.astyanax;

import com.bazaarvoice.emodb.common.api.Ttls;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.event.api.ChannelConfiguration;
import com.bazaarvoice.emodb.event.core.MetricsGroupName;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;

/**
 * Saves slab information to the slab 'manifest' column family.
 */
public class AstyanaxManifestPersister implements ManifestPersister {
    private final CassandraKeyspace _keyspace;
    private final ChannelConfiguration _channelConfiguration;
    private final Meter _openMeter;
    private final Meter _deleteMeter;

    @Inject
    public AstyanaxManifestPersister(CassandraKeyspace keyspace, ChannelConfiguration channelConfiguration,
                                     @MetricsGroupName String metricsGroup, MetricRegistry metricRegistry) {
        _keyspace = keyspace;
        _channelConfiguration = channelConfiguration;

        _openMeter = metricRegistry.meter(MetricRegistry.name(metricsGroup, "AstyanaxManifestPersister", "open"));
        _deleteMeter = metricRegistry.meter(MetricRegistry.name(metricsGroup, "AstyanaxManifestPersister", "delete"));
    }

    @Override
    public void open(String channel, ByteBuffer slabId) {
        // Updates on open must be durable or we'll lose slabs.
        save(channel, slabId, true, ConsistencyLevel.CL_LOCAL_QUORUM);

        _openMeter.mark();
    }

    @Override
    public void close(String channel, ByteBuffer slabId) {
        // Updates on close don't need to be particularly durable since open slabs will time out automatically if
        // not closed explicitly.
        save(channel, slabId, false, ConsistencyLevel.CL_ANY);

        // Don't meter close calls.  Callers can track close calls w/metrics specific to the reason for the close.
    }

    private void save(String channel, ByteBuffer slabId, boolean open, ConsistencyLevel consistency) {
        MutationBatch mutation = _keyspace.prepareMutationBatch(consistency);

        Duration ttl = getTtl(channel, open);
        mutation.withRow(ColumnFamilies.MANIFEST, channel)
                .putColumn(slabId, open, Ttls.toSeconds(ttl, 1, null));

        // Readers check for the open slab marker to see if a slab is open and may not re-read the manifest open
        // flag very often.  So delete the open slab marker so readers notice the state change more quickly.
        if (!open) {
            mutation.withRow(ColumnFamilies.SLAB, slabId)
                    .deleteColumn(Constants.OPEN_SLAB_MARKER);
        }

        execute(mutation);
    }

    @Override
    public void delete(String channel, ByteBuffer slabId) {
        // Deletes don't need to be durable.  If a delete is lost, the next reader to come along and find no events
        // will execute the delete again.
        MutationBatch mutation = _keyspace.prepareMutationBatch(ConsistencyLevel.CL_ANY);

        mutation.withRow(ColumnFamilies.MANIFEST, channel)
                .deleteColumn(slabId);

        mutation.withRow(ColumnFamilies.SLAB, slabId)
                .delete();

        execute(mutation);

        _deleteMeter.mark();
    }

    @Override
    public void move(String fromChannel, String toChannel, Collection<ByteBuffer> slabIds, boolean open) {
        // This moves a set of closed slabs from one channel to another.  Moving open slabs is not implemented
        // since it causes race conditions with open writers.
        if (open) {
            throw new UnsupportedOperationException();
        }

        // Write the new data to "toChannel".
        MutationBatch toMutation = _keyspace.prepareMutationBatch(ConsistencyLevel.CL_LOCAL_QUORUM);
        Integer ttlSeconds = Ttls.toSeconds(getTtl(toChannel, open), 1, null);
        ColumnListMutation<ByteBuffer> toRow = toMutation.withRow(ColumnFamilies.MANIFEST, toChannel);
        for (ByteBuffer slabId : slabIds) {
            toRow.putColumn(slabId, open, ttlSeconds);
        }
        execute(toMutation);

        // Once the new data is safely written we can delete from "fromChannel".
        MutationBatch fromMutation = _keyspace.prepareMutationBatch(ConsistencyLevel.CL_LOCAL_QUORUM);
        ColumnListMutation<ByteBuffer> fromRow = fromMutation.withRow(ColumnFamilies.MANIFEST, fromChannel);
        for (ByteBuffer slabId : slabIds) {
            fromRow.deleteColumn(slabId);
        }
        execute(fromMutation);
    }

    private Duration getTtl(String channel, boolean open) {
        // When a slab is open set its TTL for an extra hour to make sure the manifest entry doesn't expire before the
        // events it points to.  When a slab is closed, set it to the regular event TTL.  There's no point in it
        // outliving the events it contains.
        Duration eventTtl = _channelConfiguration.getEventTtl(channel);
        return open ? eventTtl.plus(Constants.SLAB_ROTATE_TTL) : eventTtl;
    }

    private <R> R execute(Execution<R> execution) {
        OperationResult<R> operationResult;
        try {
            operationResult = execution.execute();
        } catch (ConnectionException e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        return operationResult.getResult();
    }
}
