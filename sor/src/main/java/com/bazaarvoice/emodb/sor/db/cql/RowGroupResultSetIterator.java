package com.bazaarvoice.emodb.sor.db.cql;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.Iterator;

abstract public class RowGroupResultSetIterator extends AbstractIterator<RowGroup> {

    // The source result set
    private Supplier<ResultSet> _resultSetSupplier;
    private ResultSet _resultSet;
    private final int _prefetchLimit;
    private Object _currentKey;
    private Row _firstRowOfCurrentGroup = null;
    private boolean _currentGroupFullyIterated = true;

    private volatile ListenableFuture<ResultSet> _prefetchFuture = null;
    private final Runnable _onPrefetchComplete = new Runnable() {
        @Override
        public void run() {
            // Reset the future to null to mark the prefetch is complete
            _prefetchFuture = null;
        }
    };

    abstract protected Object getKeyForRow(Row row);

    abstract protected ResultSet queryRowGroupRowsAfter(Row row);

    public RowGroupResultSetIterator(ResultSet resultSet, int prefetchLimit) {
        this(Suppliers.ofInstance(resultSet), prefetchLimit);
    }

    public RowGroupResultSetIterator(final ListenableFuture<ResultSet> resultSetFuture, int prefetchLimit) {
        this(() -> Futures.getUnchecked(resultSetFuture), prefetchLimit);
    }

    private RowGroupResultSetIterator(Supplier<ResultSet> resultSetSupplier, int prefetchLimit) {
        _resultSetSupplier = resultSetSupplier;
        _prefetchLimit = prefetchLimit;
    }

    @Override
    protected RowGroup computeNext() {
        if (_resultSet == null) {
            // First time next() or hasNext() has been called; resolve the result set
            _resultSet = _resultSetSupplier.get();
            // Supplier is no longer necessary; dereference to allow garbage collection
            _resultSetSupplier = null;
        }

        if (!_currentGroupFullyIterated) {
            throw new IllegalStateException("Cannot start the next group until the last returned group is fully iterated");
        }

        if (_firstRowOfCurrentGroup == null) {
            if (_resultSet.isExhausted()) {
                // Either the result set contained no rows or the last group contained the final row
                return endOfData();
            }
            // First read, get the initial row
            setFirstRowOfNextGroup(nextRow());
        }

        // Mark that the current group has not been fully iterated
        _currentGroupFullyIterated = false;

        return new RowGroupImpl();
    }

    private class RowGroupImpl extends AbstractIterator<Row> implements RowGroup {
        private Row _currentRow = null;

        @Override
        protected Row computeNext() {
            if (_currentRow == null) {
                // First iteration, return the already fetched first row for this group
                _currentRow = _firstRowOfCurrentGroup;
                return _currentRow;
            }

            _currentRow = nextRow();

            if (_currentRow == null || !Objects.equal(_currentKey, getKeyForRow(_currentRow))) {
                // End of the complete result set or the start of a new group
                setFirstRowOfNextGroup(_currentRow);
                // The current group has now been fully iterated
                _currentGroupFullyIterated = true;
                return endOfData();
            }

            return _currentRow;
        }

        @Override
        public Iterator<Row> reloadRowsAfter(Row row) {
            return queryRowGroupRowsAfter(row).iterator();
        }
    }

    private Row nextRow() {
        // If we've reached the prefetch limit and there are more results to fetch then prefetch them now.
        if (_prefetchFuture == null && _resultSet.getAvailableWithoutFetching() <= _prefetchLimit && !_resultSet.isFullyFetched()) {
            _prefetchFuture = _resultSet.fetchMoreResults();
            _prefetchFuture.addListener(_onPrefetchComplete, MoreExecutors.sameThreadExecutor());
        }

        return _resultSet.one();
    }

    private void setFirstRowOfNextGroup(Row row) {
        // This call is made immediately prior to fully iterating the current group, so after this call is made
        // the next group will become the current group.
        _firstRowOfCurrentGroup = row;

        if (row != null) {
            // Set the new current key
            _currentKey = getKeyForRow(row);
        }
    }
}
