package com.bazaarvoice.emodb.common.stash;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Throwables;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * InputStream for S3 which will attempt to restart the stream if there is an error or it is unexpectedly closed.
 */
public class RestartingS3InputStream extends InputStream {

    private final AmazonS3 _s3;
    private final String _bucket;
    private final String _key;
    private long _length;
    private InputStream _in;
    private long _pos;

    public RestartingS3InputStream(AmazonS3 s3, String bucket, String key) {
        this(s3, bucket, key, null);
    }

    public RestartingS3InputStream(AmazonS3 s3, String bucket, String key, @Nullable Range<Long> range) {
        _s3 = s3;
        _bucket = bucket;
        _key = key;

        S3Object s3Object;

        // Get the object synchronously so any immediate S3 errors, such as file not found, are thrown inline.
        if (range == null) {
            s3Object = _s3.getObject(_bucket, _key);
            _pos = 0;
            _length = s3Object.getObjectMetadata().getContentLength();
        } else {
            long start, end;

            if (range.hasLowerBound()) {
                start = range.lowerEndpoint() + (range.lowerBoundType() == BoundType.CLOSED ? 0 : 1);
            } else {
                start = 0;
            }

            if (range.hasUpperBound()) {
                end = range.upperEndpoint() - (range.upperBoundType() == BoundType.CLOSED ? 0 : 1);
            } else {
                end = Long.MAX_VALUE;
            }

            s3Object = _s3.getObject(new GetObjectRequest(_bucket, _key).withRange(start, end));

            _pos = start;
            // The S3 metadata's content length is the length of the data actually being returned by S3.
            // Since we effectively skipped the first "start" bytes we need to add them back to the total length
            // of data being read to make future calculations using _pos and _length consistent.
            _length = start + s3Object.getObjectMetadata().getContentLength();
        }

        _in = s3Object.getObjectContent();
    }

    @Override
    public int read() throws IOException {
        IOException exception = null;
        boolean firstAttempt = true;

        while (exception == null) {
            try {
                int b = _in.read();
                if (b != -1) {
                    _pos += 1;
                } else if (_pos < _length) {
                    // Should be more data.  Likely the other side closed the connection, such as due to timeout,
                    // or the connection was lost.
                    throw new EOFException("Unexpected end of input");
                }
                return b;
            } catch (IOException e) {
                if (firstAttempt) {
                    // Re-open the stream and try again.
                    reopenS3InputStream();
                    firstAttempt = false;
                } else {
                    // Pass the exception along to the caller
                    exception = e;
                }
            }
        }

        throw exception;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        IOException exception = null;
        boolean firstAttempt = true;

        while (exception == null) {
            try {
                int bytesRead = _in.read(b, off, len);
                if (bytesRead != -1) {
                    _pos += bytesRead;
                } else if (_pos < _length) {
                    // Should be more data.  Likely the other side closed the connection, such as due to timeout,
                    // or the connection was lost.
                    throw new EOFException("Unexpected end of input");
                }

                return bytesRead;
            } catch (IOException e) {
                if (firstAttempt) {
                    // Re-open the stream and try again.
                    reopenS3InputStream();
                    firstAttempt = false;
                } else {
                    // Pass the exception along to the caller
                    exception = e;
                }
            }
        }

        throw exception;
    }

    /**
     * Re-opens the input stream, starting at the first unread byte.
     */
    private void reopenS3InputStream()
            throws IOException {
        // First attempt to close the existing input stream
        try {
            closeS3InputStream();
        } catch (IOException ignore) {
            // Ignore this exception; we're re-opening because there was in issue with the existing stream
            // in the first place.
        }

        InputStream remainingIn = null;
        int attempt = 0;
        while (remainingIn == null) {
            try {
                S3Object s3Object = _s3.getObject(
                        new GetObjectRequest(_bucket, _key)
                                .withRange(_pos, _length - 1));  // Range is inclusive, hence length-1

                remainingIn = s3Object.getObjectContent();
            } catch (AmazonClientException e) {
                // Allow up to 3 retries
                attempt += 1;
                if (!e.isRetryable() || attempt == 4) {
                    throw e;
                }
                // Back-off on each retry
                try {
                    Thread.sleep(200 * attempt);
                } catch (InterruptedException interrupt) {
                    throw Throwables.propagate(interrupt);
                }
            }
        }

        _in = remainingIn;
    }

    private void closeS3InputStream()
            throws IOException {
        if (_in != null) {
            _in.close();
            _in = null;
        }
    }

    @Override
    public void close() throws IOException {
        closeS3InputStream();
    }
}
