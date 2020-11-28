package com.bazaarvoice.emodb.common.json;

import org.slf4j.Logger;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

/**
 * Logs exceptions that would otherwise go unlogged while streaming results back to the client.
 * <p>
 * Exceptions thrown from the client while streaming results back to the browser tend to be converted
 * to {@link com.fasterxml.jackson.databind.JsonMappingException}.  Since that exception extends
 * {@link java.io.IOException} the Jetty catch clause in {@code org.eclipse.jetty.servlet.ServletHandler}
 * logs the exception to the {@code DEBUG} channel where it gets lost.
 */
public class LoggingIterator<T> implements Iterator<T> {
    private final Iterator<T> _delegate;
    private final Logger _log;
    private long _count;

    public LoggingIterator(Iterator<T> delegate, Logger log) {
        _delegate = StreamSupport.stream(toCheckedSpliterator(delegate), false).iterator();
        _log = log;
    }

    private Spliterator<T> toCheckedSpliterator(Iterator<T> iterator) {
        final Spliterator<T> delegate = Spliterators.spliteratorUnknownSize(iterator, 0);
        return new Spliterators.AbstractSpliterator<T>(delegate.estimateSize(), delegate.characteristics()) {
            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                try {
                    boolean advanced = delegate.tryAdvance(action);
                    if (advanced) {
                        _count++;
                    }
                    return advanced;
                } catch (Exception e) {
                    _log.error("Unexpected exception iterating through content on item #{}.", _count, e);
                    // Propagate the exception.  Unfortunately ObjectMapper will close the JsonGenerator anyway such that
                    // clients receive a well-formed json array response but with elements missing.  There doesn't seem to
                    // be a good way from here to communicate to the client that something went wrong.
                    throw e;
                }
            }
        };
    }

    @Override
    public boolean hasNext() {
        return _delegate.hasNext();
    }

    @Override
    public T next() {
        return _delegate.next();
    }
}
