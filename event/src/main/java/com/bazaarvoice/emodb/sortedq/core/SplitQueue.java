package com.bazaarvoice.emodb.sortedq.core;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import java.util.LinkedHashMap;
import java.util.Set;

/**
 * A first-in-first-out (FIFO) queue of objects.
 * <p>
 * The queue has two noteworthy features:
 * <ol>
 * <li> The {@link #remove(T)} method is O(1) and is the only operation that removes from the queue.
 * <li> The {@link #prioritize(T)} method temporarily moves an object to the head of the queue until a
 *      different object is prioritized instead, subverting the usual FIFO ordering.
 * </ol>
 */
class SplitQueue<T> {
    private final LinkedHashMap<T, T> _queue = new LinkedHashMap<>(16, 0.75f, true);
    private T _prioritize;

    /** Adds an object at the tail of the queue. */
    void offer(T obj) {
        _queue.put(obj, obj);
    }

    /** Removes an object from the queue. */
    void remove(T obj) {
        _queue.remove(obj);
        if (_prioritize == obj) {
            _prioritize = null;
        }
    }

    /** Bumps an item to the head of the queue until it is removed or until another object is prioritized instead. */
    void prioritize(T obj) {
        // LinkedHashMap always puts to the tail of the queue.  We can't re-order its linked list in a useful way.
        // So implement prioritize by simply remembering the most recent prioritized object.
        if (_queue.containsKey(obj)) {
            _prioritize = obj;
        }
    }

    /** Returns the head of the queue, then cycles it to the back of the queue. */
    T cycle() {
        if (_prioritize != null) {
            return _prioritize;
        }
        if (!_queue.isEmpty()) {
            T first = _queue.keySet().iterator().next();
            _queue.get(first);  // Access the object so it gets bumped to the back of the queue.
            return first;
        }
        return null;
    }

    boolean isEmpty() {
        return _queue.isEmpty();
    }

    // For debugging
    @Override
    public String toString() {
        Set<T> prioritized = Optional.fromNullable(_prioritize).asSet();
        return "SplitQueue[" + Joiner.on(',').join(Sets.union(prioritized, _queue.keySet())) + "]";
    }
}
