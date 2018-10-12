package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.ChangeBuilder;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;
import java.util.UUID;

/**
 * Similar to {@link Iterators#mergeSorted(Iterable, java.util.Comparator)} but specialized for {@link Change}
 * objects which should be combined when changes have the same changeId.
 */
public class MergeIterator extends AbstractIterator<Change> {
    private final PeekingIterator<Change> _iter1;
    private final PeekingIterator<Change> _iter2;
    private final Ordering<UUID> _ordering;

    public static Iterator<Change> merge(Iterator<Change> iter1, Iterator<Change> iter2, boolean reversed) {
        if (!iter2.hasNext()) {
            return iter1;
        } else if (!iter1.hasNext()) {
            return iter2;
        }
        Ordering<UUID> ordering = TimeUUIDs.ordering();
        return new MergeIterator(iter1, iter2, reversed ? ordering.reverse() : ordering);
    }

    private MergeIterator(Iterator<Change> iter1, Iterator<Change> iter2, Ordering<UUID> ordering) {
        _iter1 = Iterators.peekingIterator(iter1);
        _iter2 = Iterators.peekingIterator(iter2);
        _ordering = ordering;
    }

    @Override
    protected Change computeNext() {
        if (_iter1.hasNext() && _iter2.hasNext()) {
            UUID id1 = _iter1.peek().getId();
            UUID id2 = _iter2.peek().getId();
            UUID minId = _ordering.min(id1, id2);
            ChangeBuilder builder = new ChangeBuilder(minId);
            if (minId.equals(id1)) {
                builder.merge(_iter1.next());
            }
            if (minId.equals(id2)) {
                builder.merge(_iter2.next());
            }
            return builder.build();
        } else if (_iter1.hasNext()) {
            return _iter1.next();
        } else if (_iter2.hasNext()) {
            return _iter2.next();
        } else {
            return endOfData();
        }
    }
}
