package com.bazaarvoice.emodb.common.stash;

import com.amazonaws.services.s3.AmazonS3;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.Closeables;
import com.google.common.io.LineReader;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Closeable iterator for Stash splits.
 */
class StashSplitIterator extends AbstractIterator<Map<String, Object>> implements StashRowIterator {
    private final AtomicBoolean _closed = new AtomicBoolean(false);
    private final BufferedReader _in;
    private final LineReader _reader;

    StashSplitIterator(AmazonS3 s3, String bucket, String key) {
        InputStream rawIn = new RestartingS3InputStream(s3, bucket, key);
        try {
            // File is gzipped
            // Note:
            //   Because the content may be concatenated gzip files we cannot use the default GZIPInputStream.
            //   GzipCompressorInputStream supports concatenated gzip files.
            GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(rawIn, true);
            _in = new BufferedReader(new InputStreamReader(gzipIn, Charsets.UTF_8));
            // Create a line reader
            _reader = new LineReader(_in);
        } catch (Exception e) {
            try {
                Closeables.close(rawIn, true);
            } catch (IOException ignore) {
                // Won't happen, already caught and logged
            }
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected Map<String, Object> computeNext() {
        String line;
        try {
            line = _reader.readLine();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        if (line == null) {
            try {
                close();
            } catch (IOException ignore) {
                // Don't worry about this, we're done iterating anyway
            }
            return endOfData();
        }

        //noinspection unchecked
        return JsonHelper.fromJson(line, Map.class);
    }

    @Override
    public void close()
            throws IOException {
        if (_closed.compareAndSet(false, true)) {
            _in.close();
        }
    }
    /*
    @Override
    protected void finalize() throws Throwable {
        super.StashReader();
        close();
    }*/
}
