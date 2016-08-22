package com.bazaarvoice.emodb.event.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Helper for writing a bunch of rows and columns that coalesces writes together and flushes periodically.
 */
public class BatchUpdate {
    private final CassandraKeyspace _keyspace;
    private final ConsistencyLevel _consistencyLevel;
    private final int _maxRows;
    private final int _maxColumns;
    private final List<Future<OperationResult<Void>>> _futures = Lists.newArrayList();
    private MutationBatch _mutation;
    private int _numRows;
    private int _numColumns;
    private boolean _open = true;

    public BatchUpdate(CassandraKeyspace keyspace, ConsistencyLevel consistencyLevel, int maxRows, int maxColumns) {
        checkArgument(maxRows > 0);
        checkArgument(maxColumns > 0);
        _keyspace = checkNotNull(keyspace);
        _consistencyLevel = checkNotNull(consistencyLevel);
        _maxRows = maxRows;
        _maxColumns = maxColumns;
    }

    public <K, C> Row<K, C> updateRow(ColumnFamily<K, C> columnFamily, K rowKey) {
        return updateRow(columnFamily, rowKey, null);
    }

    public <K, C> Row<K, C> updateRow(ColumnFamily<K, C> columnFamily, K rowKey,
                                      @Nullable Function<Row<K, C>, Void> newRowCallback) {
        return new Row<>(columnFamily, rowKey, newRowCallback);
    }

    private MutationBatch mutation() {
        checkState(_open);
        if (_mutation == null) {
            _mutation = _keyspace.prepareMutationBatch(_consistencyLevel);
        }
        return _mutation;
    }

    private void flushIf(boolean condition) {
        if (condition && _mutation != null) {
            _futures.add(executeAsync(_mutation));
            _mutation = null;
            _numRows = 0;
            _numColumns = 0;
        }
    }

    public void finish() {
        _open = false;
        flushIf(true);
        // Wait for writes to complete and check that they all succeeded
        for (Future<OperationResult<Void>> future : _futures) {
            Futures.getUnchecked(future);
        }
    }

    private <R> Future<OperationResult<R>> executeAsync(Execution<R> execution) {
        try {
            return execution.executeAsync();
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }
    }

    public class Row<K, C> {
        private final ColumnFamily<K, C> _columnFamily;
        private final Function<Row<K, C>, Void> _newRowCallback;
        private final K _rowKey;
        private MutationBatch _mutation;
        private ColumnListMutation<C> _row;

        Row(ColumnFamily<K, C> columnFamily, K rowKey, Function<Row<K, C>, Void> newRowCallback) {
            _columnFamily = checkNotNull(columnFamily);
            _rowKey = checkNotNull(rowKey);
            _newRowCallback = newRowCallback;
        }

        private ColumnListMutation<C> row() {
            // If this is the first column in the row or after a flush, setup the mutation and row objects.
            if (_mutation != mutation()) {
                flushIf(_numRows >= _maxRows);
                _mutation = mutation();
                _row = _mutation.withRow(_columnFamily, _rowKey);
                _numRows++;
                if (_newRowCallback != null) {
                    _newRowCallback.apply(this);
                }
            }
            return _row;
        }

        public void putColumn(C columnKey, ByteBuffer value, Integer ttlSeconds) {
            row().putColumn(columnKey, value, ttlSeconds);
            flushIf(++_numColumns >= _maxColumns);
        }

        public void deleteColumn(C columnKey) {
            row().deleteColumn(columnKey);
            flushIf(++_numColumns >= _maxColumns);
        }

        public void deleteRow() {
            row().delete();
        }
    }
}
