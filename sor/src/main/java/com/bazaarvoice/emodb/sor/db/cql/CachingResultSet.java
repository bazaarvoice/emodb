package com.bazaarvoice.emodb.sor.db.cql;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * CachingResultSet is an extension of ResultSet with the following two enhancements
 * 1. It caches the rows while only passing through the iterator once.
 * 2. Calling iterator() gives a fresh iterator every time, and not an already consumed one, with the following caveats:
 *      - Not thread safe. Do not traverse iterators in parallel.
 *      - Once any of the created iterators have started traversing, new iterators are not allowed.
 */
public class CachingResultSet implements Iterable<Row> {

    private final List<Queue<Row>> _backingLists;
    private final Iterator<Row> _resultSetIter;
    private boolean _init = false;

    public CachingResultSet(ResultSetSupplier resultSetSupplier) {
        _backingLists = Lists.newArrayList();

        checkNotNull(resultSetSupplier, "resultSetSupplier");
        ResultSet resultSet;
        if (resultSetSupplier.isAsync()) {
            resultSet = resultSetSupplier.getFromFuture();
        } else {
            resultSet = resultSetSupplier.get(null);
        }
        checkState(resultSet != null, "ResultSet is not fetched");
        //noinspection ConstantConditions
        _resultSetIter = resultSet.iterator();
    }

    @Override
    public Iterator<Row> iterator() {
        // Make sure that the iterators haven't started traversing yet
        checkState(!_init, "Can't create more cached iterators once iterators have moved on!");
        // Add a backing list first;
        final Queue<Row> myBackingList = new LinkedList<>();
        // Register with the list of queues
        _backingLists.add(myBackingList);

        return new AbstractIterator<Row>() {
            @Override
            protected Row computeNext() {
                if (!_init) {
                    _init = true;
                }
                if (!myBackingList.isEmpty()) {
                    return myBackingList.poll();
                }
                if (!_resultSetIter.hasNext()) {
                    return endOfData();
                }
                Row row = _resultSetIter.next();

                for (Queue<Row> queue : _backingLists) {
                    queue.add(row);
                }

                return myBackingList.poll();
            }
        };
    }

}
