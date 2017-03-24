package com.bazaarvoice.emodb.event.tracer;

import com.bazaarvoice.emodb.event.api.EventTracer;
import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Helper base implementation of {@link EventTracer} which streams the trace content to a CSV.  Subclasses must implement
 * the method for converting raw event bytes into CSV columns.
 */
abstract public class CSVEventTracer implements EventTracer {

    private final Writer _out;
    private final boolean _closeWriterOnClose;
    private final CharMatcher _requiresEscapeMatcher = CharMatcher.anyOf("\",");

    public CSVEventTracer(Writer out, boolean closeWriterOnClose) {
        _out = checkNotNull(out, "out");
        _closeWriterOnClose = closeWriterOnClose;
    }

    /**
     * Given the event source and data from a call to {@link #trace(String, ByteBuffer)}, return the CSV columns in
     * order of inclusion.
     */
    abstract protected List<Object> toColumns(String source, ByteBuffer data);

    @Override
    public void trace(String source, ByteBuffer data) {
        try {
            for (Iterator<Object> columnIterator = toColumns(source, data).iterator(); columnIterator.hasNext(); ) {
                Object column = columnIterator.next();
                if (column != null) {
                    String value = column.toString();
                    if (_requiresEscapeMatcher.matchesAnyOf(value)) {
                        value = String.format("\"%s\"", value.replaceAll("\"", "\"\""));
                    }
                    _out.write(value);
                }
                if (columnIterator.hasNext()) {
                    _out.write(",");
                }
            }
            _out.write("\n");
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (_closeWriterOnClose) {
            _out.flush();
            _out.close();
        }
    }
}
