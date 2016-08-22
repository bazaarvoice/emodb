package com.bazaarvoice.emodb.common.json;

import java.util.Iterator;

/**
 * Helper for {@link RestartingStreamingIterator} that supplies the underlying streaming iterators.
 */
public interface StreamingIteratorSupplier<T, K> {
    Iterator<T> get(K fromToken, long limit);

    K getNextToken(T object);
}
