package com.bazaarvoice.emodb.common.stash;

import com.amazonaws.services.s3.AmazonS3;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.streaming.AbstractSpliterator;
import com.bazaarvoice.emodb.streaming.SpliteratorIterator;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Closeable iterator for Stash splits.
 */
class StashSplitIterator extends SpliteratorIterator<Map<String, Object>> implements StashRowIterator {
    private final AtomicBoolean _closed = new AtomicBoolean(false);
    private final BufferedReader _in;

    @Override
    protected Spliterator<Map<String, Object>> getSpliterator() {
        return new AbstractSpliterator<Map<String, Object>>() {
            @Override
            protected Map<String, Object> computeNext() {
                String line;
                try {
                    line = _in.readLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                if (line == null) {
                    try {
                        close();
                    } catch (IOException ignore) {
                        // Don't worry about this, we're done iterating anyway
                    }
                    return endOfStream();
                }

                //noinspection unchecked
                return JsonHelper.fromJson(line, Map.class);
            }
        };
    }

    StashSplitIterator(AmazonS3 s3, String bucket, String key) {
        InputStream rawIn = new RestartingS3InputStream(s3, bucket, key);
        try {
            // File is gzipped
            // Note:
            //   Because the content may be concatenated gzip files we cannot use the default GZIPInputStream.
            //   GzipCompressorInputStream supports concatenated gzip files.
            GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(rawIn, true);
            _in = new BufferedReader(new InputStreamReader(gzipIn, Charset.forName("UTF-8")));
        } catch (Exception e) {
            try {
                rawIn.close();
            } catch (IOException ignore) {
                // ignore
            }
            throw (e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e));
        }
    }

    @Override
    public void close()
            throws IOException {
        if (_closed.compareAndSet(false, true)) {
            _in.close();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }
}
