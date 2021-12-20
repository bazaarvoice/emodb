package com.bazaarvoice.emodb.common.dropwizard.metrics;

import com.codahale.metrics.Counter;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

/**
 * OutputStream wrapper that updates a counter metric with the number of bytes written to the stream.
 */
public class MetricCounterOutputStream extends FilterOutputStream {

    private final static long FLUSH_COUNT = 20 * 1024 * 1024;  // 20 MB
    private final Counter _counter;
    private long _bufferedCount;

    public MetricCounterOutputStream(OutputStream delegate, Counter counter) {
        super(requireNonNull(delegate, "delegate"));
        _counter = requireNonNull(counter, "counter");
    }

    @Override
    public void write(int b)
            throws IOException {
        out.write(b);
        inc(1);
    }

    @Override
    public void write(byte[] b)
            throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException {
        out.write(b, off, len);
        inc(len);
    }

    private void inc(long count) {
        // Writing directly to the counter is expensive if there are a lot of small writes.  Instead buffer
        // counts locally and flush after the FLUSH_COUNT threshold is met.
        _bufferedCount += count;
        if (_bufferedCount >= FLUSH_COUNT) {
            _counter.inc(_bufferedCount);
            _bufferedCount = 0;
        }
    }

    @Override
    public void close()
            throws IOException {
        // Flush any final buffered counts to the counter.
        if (_bufferedCount > 0) {
            _counter.inc(_bufferedCount);
            _bufferedCount = 0;
        }
        out.close();
    }
}
