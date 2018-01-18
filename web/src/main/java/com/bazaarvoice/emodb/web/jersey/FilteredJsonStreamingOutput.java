package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * Custom JSON stream writer which filters items from the source iterator and injects whitespace into the returned
 * array at regular intervals of filtered content.  Once the provided maximum length of the filtered content has
 * been written it will stop.
 */
abstract public class FilteredJsonStreamingOutput<T> implements StreamingOutput {

    private final Iterator<T> _iterator;
    private final long _limit;

    public FilteredJsonStreamingOutput(Iterator<T> iterator, long limit) {
        // Force the first item to be read before streaming results, so if there are any errors they are thrown before
        // writing any content
        PeekingIterator<T> peekingIterator = Iterators.peekingIterator(iterator);
        if (peekingIterator.hasNext()) {
            peekingIterator.peek();
        }

        _iterator = peekingIterator;
        _limit = Math.max(limit, 0);
    }

    abstract public boolean include(T value);

    @Override
    public void write(OutputStream out) throws IOException, WebApplicationException {
        // The JSON writer may try to close the stream after each entry.  To protect against this use a "safe"
        // output stream which will ignore calls by the JSON writer to close.
        OutputStream safeOut = asNonClosingOutputStream(out);

        safeOut.write("[".getBytes(Charsets.UTF_8));
        byte[] sep = null;
        long nextWhitespaceTime = Long.MAX_VALUE;
        long remaining = _limit;

        while (_iterator.hasNext() && remaining != 0) {
            T value = _iterator.next();
            if (include(value)) {
                if (sep == null) {
                    // first value written
                    sep = ",".getBytes(Charsets.UTF_8);
                } else {
                    safeOut.write(sep);
                }
                JsonHelper.writeJson(safeOut, value);
                nextWhitespaceTime = Long.MAX_VALUE;
                remaining -= 1;
            } else {
                // Target writing whitespace every 100ms to keep the stream alive
                if (nextWhitespaceTime == Long.MAX_VALUE) {
                    nextWhitespaceTime = System.currentTimeMillis() + 100;
                } else if (System.currentTimeMillis() >= nextWhitespaceTime) {
                    safeOut.write(" ".getBytes(Charsets.UTF_8));
                    nextWhitespaceTime = System.currentTimeMillis() + 100;
                }
            }
        }

        safeOut.write("]".getBytes(Charsets.UTF_8));
    }

    private OutputStream asNonClosingOutputStream(OutputStream out) {
        return new FilterOutputStream(out) {
            @Override
            public void close() throws IOException {
                // Keep stream open
            }
        };
    }
}
