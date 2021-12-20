package com.bazaarvoice.emodb.common.json;

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;

import java.util.Iterator;

/**
 * Logs exceptions that would otherwise go unlogged while streaming results back to the client.
 * <p>
 * Exceptions thrown from the client while streaming results back to the browser tend to be converted
 * to {@link com.fasterxml.jackson.databind.JsonMappingException}.  Since that exception extends
 * {@link java.io.IOException} the Jetty catch clause in {@code org.eclipse.jetty.servlet.ServletHandler}
 * logs the exception to the {@code DEBUG} channel where it gets lost.
 */
public class LoggingIterator<T> extends AbstractIterator<T> {
    private final Iterator<T> _delegate;
    private final Logger _log;
    private long _count;

    public LoggingIterator(Iterator<T> delegate, Logger log) {
        _delegate = delegate;
        _log = log;
    }

    @Override
    protected T computeNext() {
        try {
            _count++;
            if (_delegate.hasNext()) {
                return _delegate.next();
            }
            return endOfData();
        } catch (Exception e) {
            _log.error("Unexpected exception iterating through content on item #{}.", _count, e);
            // Propagate the exception.  Unfortunately ObjectMapper will close the JsonGenerator anyway such that
            // clients receive a well-formed json array response but with elements missing.  There doesn't seem to
            // be a good way from here to communicate to the client that something went wrong.
            Throwables.propagateIfPossible(e);
            throw new RuntimeException(e);
        }
    }
}
