package com.bazaarvoice.emodb.common.json;

import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

/**
 * Wraps a {@link JsonStreamingArrayParser} and detects early EOF and re-starts the streaming just after the last
 * object successfully parsed.
 */
public class RestartingStreamingIterator<T, K> implements Iterator<T>, Closeable {
    private final StreamingIteratorSupplier<T, K> _iterSupplier;
    private final Iterator<T> _finalIter;
    private K _restartFromToken;
    private long _remaining;
    private Iterator<T> _iter;
    private boolean _endOfStream;

    public static <T, K> Iterable<T> stream(final @Nullable K fromToken,
                                            final long limit,
                                            final StreamingIteratorSupplier<T, K> restartFn) {
        return () -> new RestartingStreamingIterator<>(fromToken, limit, restartFn);
    }

    private RestartingStreamingIterator(@Nullable K fromToken, long limit, StreamingIteratorSupplier<T, K> iterSupplier) {
        if (iterSupplier == null) {
            throw new NullPointerException("Restart function is required");
        }
        _iterSupplier = iterSupplier;
        _restartFromToken = fromToken;
        _remaining = limit;
        _iter = _iterSupplier.get(_restartFromToken, _remaining);
        if (!(_iter instanceof Closeable)) {
            // Expected to be a JsonStreamingArrayParser
            throw new IllegalArgumentException("Iterator returned should be closeable");
        };
        _finalIter = StreamSupport.stream(asRestartingSpliterator(), false).iterator();
    }

    @Override
    public boolean hasNext() {
        return _finalIter.hasNext();
    }

    @Override
    public T next() {
        return _finalIter.next();
    }

    private Spliterator<T> asRestartingSpliterator() {
        return new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, 0) {
            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                if (!_endOfStream) {
                    T next = computeNext();
                    if (!_endOfStream) {
                        action.accept(next);
                        return true;
                    }
                }
                return false;
            }
        };
    }

    private T computeNext() {
        boolean retrying = false;
        while (_remaining > 0) {
            // All calls to the iterator must be protected by a try/catch for JsonStreamingEOFException.
            T next;
            try {
                if (!_iter.hasNext()) {
                    // swallow any exceptions since we don't care if we can't close input data
                    closeUnchecked((Closeable) _iter);
                    _endOfStream = true;
                    return null;
                }
                next = _iter.next();
            } catch (JsonStreamingEOFException e) {
                // Lost the underlying input stream.
                // swallow any exceptions since we don't care if we can't close a bad stream
                closeUnchecked((Closeable) _iter);

                // When restarting the iterator ensure we retrieve at least one value successfully to avoid the risk
                // of entering an infinite retry loop.
                if (retrying) {
                    throw e;
                }
                retrying = true;

                // This next call may fail.  If it does, let the exception propagate.  We assume it implements
                // the retry logic appropriate for re-creating the stream of input values.
                _iter = _iterSupplier.get(_restartFromToken, _remaining);
                continue;
            }

            _restartFromToken = _iterSupplier.getNextToken(next);
            _remaining--;

            return next;
        }
        // swallow any exceptions since we don't care if we can't close input data
        closeUnchecked((Closeable) _iter);
        _endOfStream = true;
        return null;
    }

    private void closeUnchecked(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                // Don't care, just log
                LoggerFactory.getLogger(getClass()).debug("Failed to close stream", e);
            }
        }
    }
    @Override
    public void close() throws IOException {
        ((Closeable) _iter).close();
    }
}
