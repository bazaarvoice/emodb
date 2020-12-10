package com.bazaarvoice.emodb.sortedq.db.astyanax;

import com.bazaarvoice.emodb.common.api.Ttls;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.event.api.ChannelConfiguration;
import com.bazaarvoice.emodb.sortedq.db.QueueDAO;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import com.netflix.astyanax.util.RangeBuilder;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;


public class AstyanaxQueueDAO implements QueueDAO {
    private static final ColumnFamily<String, UUID> CF_DEDUP_MD = new ColumnFamily<>("dedup_md",
            StringSerializer.get(), TimeUUIDSerializer.get());
    private static final ColumnFamily<UUID, ByteBuffer> CF_DEDUP_DATA = new ColumnFamily<>("dedup_data",
            TimeUUIDSerializer.get(), ByteBufferSerializer.get());

    private static ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private final CassandraKeyspace _keyspace;
    private final ChannelConfiguration _channelConfiguration;

    @Inject
    public AstyanaxQueueDAO(CassandraKeyspace keyspace, ChannelConfiguration channelConfiguration) {
        _keyspace = Objects.requireNonNull(keyspace, "keyspace");
        _channelConfiguration = Objects.requireNonNull(channelConfiguration, "channelConfiguration");
    }

    @Override
    public Iterator<String> listQueues() {
        final Iterator<Row<String, UUID>> rowIter = execute(
                _keyspace.prepareQuery(CF_DEDUP_MD, ConsistencyLevel.CL_LOCAL_QUORUM)
                        .getAllRows()
                        .setRowLimit(100)
                        .withColumnRange(new RangeBuilder().setLimit(1).build()))
                .iterator();
        return new AbstractIterator<String>() {
            @Override
            protected String computeNext() {
                while (rowIter.hasNext()) {
                    Row<String, UUID> row = rowIter.next();
                    if (!row.getColumns().isEmpty()) {
                        return row.getKey();
                    }
                }
                return endOfData();
            }
        };
    }

    @Override
    public Map<UUID, String> loadSegments(String queue) {
        Map<UUID, String> resultMap = Maps.newHashMap();
        Iterator<Column<UUID>> iter = executePaginated(
                _keyspace.prepareQuery(CF_DEDUP_MD, ConsistencyLevel.CL_LOCAL_QUORUM)
                        .getKey(queue)
                        .withColumnRange(new RangeBuilder().setLimit(100).build())
                        .autoPaginate(true));
        while (iter.hasNext()) {
            Column<UUID> column = iter.next();
            resultMap.put(column.getName(), column.getStringValue());
        }
        return resultMap;
    }

    @Nullable
    @Override
    public ByteBuffer findMinRecord(UUID dataId, @Nullable ByteBuffer from) {
        // Use a column range with a "start" to skip past tombstones.
        ColumnList<ByteBuffer> columns = execute(_keyspace.prepareQuery(CF_DEDUP_DATA, ConsistencyLevel.CL_LOCAL_QUORUM)
                .getKey(dataId)
                .withColumnRange(new RangeBuilder()
                        .setStart(Optional.ofNullable(from).orElse(EMPTY_BUFFER))
                        .setLimit(1)
                        .build()));
        return !columns.isEmpty() ? columns.getColumnByIndex(0).getName() : null;
    }

    @Override
    public Map<UUID, ByteBuffer> findMaxRecords(Collection<UUID> dataIds) {
        // Finding the max using a reversed column range shouldn't have to worry about skipping tombstones since
        // we always delete smaller column values before deleting larger column values--scanning will hit the max
        // before needing to skip over tombstones.
        Map<UUID, ByteBuffer> resultMap = Maps.newHashMap();
        for (List<UUID> batch : Iterables.partition(dataIds, 10)) {
            Rows<UUID, ByteBuffer> rows = execute(
                    _keyspace.prepareQuery(CF_DEDUP_DATA, ConsistencyLevel.CL_LOCAL_QUORUM)
                            .getKeySlice(batch)
                            .withColumnRange(new RangeBuilder()
                                    .setReversed(true)
                                    .setLimit(1)
                                    .build()));
            for (Row<UUID, ByteBuffer> row : rows) {
                UUID dataId = row.getKey();
                for (Column<ByteBuffer> column : row.getColumns()) {
                    resultMap.put(dataId, column.getName());
                }
            }
        }
        return resultMap;
    }

    @Override
    public Iterator<ByteBuffer> scanRecords(UUID dataId, @Nullable ByteBuffer from, @Nullable final ByteBuffer to,
                                            int batchSize, int limit) {
        final Iterator<Column<ByteBuffer>> iter = executePaginated(
                _keyspace.prepareQuery(CF_DEDUP_DATA, ConsistencyLevel.CL_LOCAL_QUORUM)
                        .getKey(dataId)
                        .withColumnRange(new RangeBuilder()
                                .setStart(Optional.ofNullable(from).orElse(EMPTY_BUFFER))
                                .setEnd(Optional.ofNullable(to).orElse(EMPTY_BUFFER))
                                .setLimit(batchSize)
                                .build())
                        .autoPaginate(true));

        return Iterators.limit(new AbstractIterator<ByteBuffer>() {
            @Override
            protected ByteBuffer computeNext() {
                while (iter.hasNext()) {
                    ByteBuffer record = iter.next().getName();
                    if (!record.equals(to)) {  // To is exclusive
                        return record;
                    }
                }
                return endOfData();
            }
        }, limit);
    }

    @Override
    public UpdateRequest prepareUpdate(final String queue) {

        return new UpdateRequest() {
            private final MutationBatch _mutation = _keyspace.prepareMutationBatch(ConsistencyLevel.CL_LOCAL_QUORUM);
            private final Duration _eventTtl = _channelConfiguration.getEventTtl(queue);

            @Override
            public UpdateRequest writeSegment(UUID segment, String internalState) {
                Duration manifestTtl = Duration.ofDays(31);
                _mutation.withRow(CF_DEDUP_MD, queue)
                        .putColumn(segment, internalState, Ttls.toSeconds(manifestTtl, 1, null));
                return this;
            }

            @Override
            public UpdateRequest deleteSegment(UUID segment, UUID dataId) {
                _mutation.withRow(CF_DEDUP_DATA, dataId)
                        .delete();
                _mutation.withRow(CF_DEDUP_MD, queue)
                        .deleteColumn(segment);
                return this;
            }

            @Override
            public UpdateRequest writeRecords(UUID dataId, Collection<ByteBuffer> records) {
                if (!records.isEmpty()) {
                    ColumnListMutation<ByteBuffer> row = _mutation.withRow(CF_DEDUP_DATA, dataId);
                    for (ByteBuffer record : records) {
                        row.putColumn(record, EMPTY_BUFFER, Ttls.toSeconds(_eventTtl, 1, null));
                    }
                }
                return this;
            }

            @Override
            public UpdateRequest deleteRecords(UUID dataId, Collection<ByteBuffer> records) {
                if (!records.isEmpty()) {
                    ColumnListMutation<ByteBuffer> row = _mutation.withRow(CF_DEDUP_DATA, dataId);
                    for (ByteBuffer record : records) {
                        row.deleteColumn(record);
                    }
                }
                return this;
            }

            @Override
            public void execute() {
                if (!_mutation.isEmpty()) {
                    AstyanaxQueueDAO.this.execute(_mutation);
                }
            }
        };  //todo
    }

    /**
     * Executes a {@code RowQuery} with {@code autoPaginate(true)} repeatedly as necessary to fetch all pages.
     */
    private <K, C> Iterator<Column<C>> executePaginated(final RowQuery<K, C> query) {
        return Iterators.concat(new AbstractIterator<Iterator<Column<C>>>() {
            @Override
            protected Iterator<Column<C>> computeNext() {
                ColumnList<C> page = execute(query);
                return !page.isEmpty() ? page.iterator() : endOfData();
            }
        });
    }

    private <R> R execute(Execution<R> execution) {
        OperationResult<R> operationResult;
        try {
            operationResult = execution.execute();
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }
        return operationResult.getResult();
    }
}
