package com.bazaarvoice.emodb.common.json;

import com.google.common.collect.AbstractIterator;
import com.google.common.io.Closeables;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Wraps a {@link JsonStreamingArrayParser} and detects early EOF and re-starts the streaming just after the last
 * object successfully parsed.
 */
public class RestartingStreamingIterator<T, K> extends AbstractIterator<T> implements Closeable {
    private final StreamingIteratorSupplier<T, K> _iterSupplier;
    private K _restartFromToken;
    private long _remaining;
    private Iterator<T> _iter;

    public static <T, K> Iterable<T> stream(final @Nullable K fromToken,
                                            final long limit,
                                            final StreamingIteratorSupplier<T, K> restartFn) {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return new RestartingStreamingIterator<>(fromToken, limit, restartFn);
            }
        };
    }

    private RestartingStreamingIterator(@Nullable K fromToken, long limit, StreamingIteratorSupplier<T, K> iterSupplier) {
        _iterSupplier = requireNonNull(iterSupplier, "restartFn");
        _restartFromToken = fromToken;
        _remaining = limit;
        _iter = _iterSupplier.get(_restartFromToken, _remaining);
        checkArgument(_iter instanceof Closeable);  // Expected to be a JsonStreamingArrayParser
    }

    @Override
    protected T computeNext() {
        boolean retrying = false;
        while (_remaining > 0) {
            // All calls to the iterator must be protected by a try/catch for JsonStreamingEOFException.
            T next;
            try {
                if (!_iter.hasNext()) {
                    try {
                        Closeables.close((Closeable) _iter, true);
                    } catch (IOException ex) {
                        // swallow any exceptions since we don't care if we can't close input data
                    }
                    return endOfData();
                }
                next = _iter.next();
            } catch (JsonStreamingEOFException e) {
                // Lost the underlying input stream.
                try {
                    Closeables.close((Closeable) _iter, true);
                } catch (IOException ex) {
                    // swallow any exceptions since we don't care if we can't close a bad stream
                }

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
        try {
            Closeables.close((Closeable) _iter, true);
        } catch (IOException ex) {
            // swallow any exceptions since we don't care if we can't close input data
        }
        return endOfData();
    }

    @Override
    public void close() throws IOException {
        ((Closeable) _iter).close();
    }
}
