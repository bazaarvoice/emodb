package com.bazaarvoice.emodb.hadoop.io;

import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import org.apache.hadoop.io.Text;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base RecordReader implementation used by both the older "mapred" and newer mapreduce" implementations of InputFormat.
 */
abstract public class BaseRecordReader implements Closeable {

    private final int _approximateSize;
    private final AtomicBoolean _closed = new AtomicBoolean(false);
    private Iterator<Map<String, Object>> _rows;
    private int _rowsRead = 0;

    public BaseRecordReader(int splitSize) {
        _approximateSize = splitSize;
    }

    public void initialize()
            throws IOException {
        _rows = getRowIterator();

    }

    /**
     * Returns an iterator of EmoDB rows as Java Maps.
     */
    abstract protected Iterator<Map<String, Object>> getRowIterator()
            throws IOException;

    /**
     * Method guaranteed to be called exactly once when this instance is closed, even if {@link #close()} is
     * called multiple times.
     */
    abstract protected void closeOnce()
            throws IOException;

    /**
     * Read the next row from the split, storing the coordinate in "key" and the content in "value".  If there are no
     * more rows then false is returned and "key" and "value" are not modified.
     * @return true if a row was read, false if there were no more rows
     */
    public boolean setNextKeyValue(Text key, Row value)
            throws IOException {
        if (!_rows.hasNext()) {
            Closeables.close(this, true);
            return false;
        }

        try {
            Map<String, Object> content = _rows.next();
            key.set(Coordinate.fromJson(content).toString());
            value.set(content);
            _rowsRead += 1;
        } catch (Exception e) {
            for (Throwable cause : Throwables.getCausalChain(e)) {
                Throwables.propagateIfInstanceOf(cause, IOException.class);
            }
            throw new IOException("Failed to read next row", e);
        }
        return true;
    }

    /**
     * Returns a rough approximate of the progress on a scale from 0 to 1
     */
    public float getProgress()
            throws IOException {
        if (!_rows.hasNext()) {
            return 1;
        } else if (_rowsRead < _approximateSize) {
            return (float) _rowsRead / _approximateSize;
        } else {
            // We've already read the expected number of rows.
            return ((float) (_approximateSize - 1) + 0.5f) / _approximateSize;
        }
    }

    public long getRowsRead() {
        return _rowsRead;
    }

    @Override
    public void close() throws IOException {
        if (_closed.compareAndSet(false, true)) {
            closeOnce();
        }
    }

    @Override
    protected void finalize()
            throws Throwable {
        super.finalize();
        Closeables.close(this, true);
    }
}
