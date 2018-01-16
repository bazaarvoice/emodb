package com.bazaarvoice.emodb.sor.db.cql;

import com.datastax.driver.core.Row;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class CachingRowGroupIterator extends AbstractIterator<Iterable<Row>> {

    private final static int DEFAULT_SOFT_CACHE_GROUP_SIZE = 10;

    private final Iterator<RowGroup> _rowGroupIterator;
    private final int _maxCacheSize;
    private final int _softCacheGroupSize;

    public CachingRowGroupIterator(Iterator<RowGroup> rowGroupIterator, int maxCacheSize) {
        this(rowGroupIterator, maxCacheSize, DEFAULT_SOFT_CACHE_GROUP_SIZE);
    }

    public CachingRowGroupIterator(Iterator<RowGroup> rowGroupIterator, int maxCacheSize, int softCacheGroupSize) {
        checkArgument(maxCacheSize > 0, "Cache must be able to cache at least one row");
        _rowGroupIterator = rowGroupIterator;
        _maxCacheSize = maxCacheSize;
        _softCacheGroupSize = softCacheGroupSize;
    }

    @Override
    protected Iterable<Row> computeNext() {
        if (!_rowGroupIterator.hasNext()) {
            return endOfData();
        }

        final RowGroup rowGroup = _rowGroupIterator.next();

        // Immediately cache up to the maximum records
        final List<Row> cache = ImmutableList.copyOf(Iterators.limit(rowGroup, _maxCacheSize));

        if (!rowGroup.hasNext()) {
            // Easy case; all rows fit into cache.
            return cache;
        }

        // Softly cache the remaining rows.  The iterable is typically quickly consumed so this cache only has to
        // remain in memory for a short time.  The cache groups rows to reduce memory utilization and reduce the
        // likelihood that garbage collection will invalidate later entries in the soft cache, while also limiting
        // the number of rows with a hard reference at any given time.  If cache is garbage-collected prior to
        // iteration we'll refetch the remaining rows from the source.

        final List<SoftReference<List<Row>>> softCache = Lists.newArrayList();
        List<Row> softCacheGroup = Lists.newArrayListWithCapacity(_softCacheGroupSize);

        while (rowGroup.hasNext()) {
            if (softCacheGroup.size() == _softCacheGroupSize) {
                softCache.add(softlyReferenced(softCacheGroup));
                softCacheGroup = Lists.newArrayListWithCapacity(_softCacheGroupSize);
            }
            softCacheGroup.add(rowGroup.next());
        }

        softCache.add(softlyReferenced(softCacheGroup));

        return () -> {
            Iterator<Row> secondaryIterator =  new AbstractIterator<Row>() {
                private Iterator<SoftReference<List<Row>>> _softGroupsIterator = softCache.iterator();
                private Iterator<Row> _currentGroupIterator = null;
                private Iterator<Row> _sourceIterator = null;

                private Row _lastCacheRow = cache.get(cache.size()-1);

                @Override
                protected Row computeNext() {
                    if (_sourceIterator != null) {
                        // A previous softly-cached row had been lost and the remaining rows are now being served
                        // from the source.
                        return getNextFromSourceIterator();
                    }

                    // Check if we are currently traversing a soft cache group
                    Row row = getNextRowFromCurrentGroupIterator();
                    if (row != null) {
                        return row;
                    }

                    // Try to load the next group from the soft cache
                    if (!_softGroupsIterator.hasNext()) {
                        return endOfData();
                    }

                    SoftReference<List<Row>> softGroupRef = _softGroupsIterator.next();
                    List<Row> softGroup = softGroupRef.get();

                    if (softGroup != null) {
                        // Soft group has not been garbage-collected; start iterating over it.  Note this will
                        // make the group contain a hard reference until it is fully iterated.
                        _currentGroupIterator = softGroup.iterator();
                        // The soft group always contains at least one record, so this will never return null
                        return getNextRowFromCurrentGroupIterator();
                    }

                    // The soft row was garbage collected.  Only option now is to reload the remainder from
                    // the backend.  Start by dereferencing all of the remaining cached row groups that can no
                    // longer be used, though keep the current row group to signal future iterations to reload
                    // remaining rows.
                    _softGroupsIterator.forEachRemaining(Reference::clear);

                    _sourceIterator = rowGroup.reloadRowsAfter(_lastCacheRow);
                    return getNextFromSourceIterator();
                }

                private Row getNextRowFromCurrentGroupIterator() {
                    if (_currentGroupIterator == null) {
                        return null;
                    } else if (!_currentGroupIterator.hasNext()) {
                        _currentGroupIterator = null;
                        return null;
                    }
                    // Record the last row returned in case the next row has been garbage collected
                    _lastCacheRow = _currentGroupIterator.next();
                    return _lastCacheRow;
                }

                private Row getNextFromSourceIterator() {
                    if (!_sourceIterator.hasNext()) {
                        return endOfData();
                    }
                    return _sourceIterator.next();
                }
            };

            return Iterators.concat(cache.iterator(), secondaryIterator);
        };
    }

    /**
     * Simple method to create a soft reference to the given row list.  Performed in a method to allow unit-tests
     * to override for controlled testing.
     */
    protected SoftReference<List<Row>> softlyReferenced(List<Row> rows) {
        return new SoftReference<>(rows);
    }
}
