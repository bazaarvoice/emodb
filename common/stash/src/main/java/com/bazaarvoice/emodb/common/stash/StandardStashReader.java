package com.bazaarvoice.emodb.common.stash;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.bazaarvoice.emodb.sor.api.StashNotAvailableException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;

import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Provides basic access to Stashed tables and content from a rotating top-level directory.  New stash content is
 * written to subdirectories and the most recent subdirectory is stored in a _LATEST file.  For example, the immediate
 * structure under the stash root may look similar to this:
 *
 *  (dir) 2015-02-01-00-00-00
 *  (dir) 2015-02-02-00-00-00
 *  (dir) 2015-02-03-00-00-00
 * (file) _LATEST
 *
 * where the contents of _LATEST is the single line "2015-02-03-00-00-00"
 */
public class StandardStashReader extends StashReader {
    // Refresh the latest available stash every 2 minutes
    private static final long REFRESH_LATEST_MS = TimeUnit.MINUTES.toMillis(2);

    private final Supplier<String> _latestRootSupplier;
    private String _lockedLatest;

    public static StandardStashReader getInstance(URI stashRoot) {
        return getInstance(stashRoot, new DefaultAWSCredentialsProviderChain(), null);
    }

    public static StandardStashReader getInstance(URI stashRoot, ClientConfiguration s3Config) {
        return getInstance(stashRoot, getS3Client(stashRoot,  new DefaultAWSCredentialsProviderChain(), s3Config));
    }

    public static StandardStashReader getInstance(URI stashRoot, String accessKey, String secretKey) {
        return getInstance(stashRoot, new StaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)), null);
    }

    public static StandardStashReader getInstance(URI stashRoot, String accessKey, String secretKey,
                                                  ClientConfiguration s3Config) {
        return getInstance(stashRoot, new StaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)), s3Config);
    }

    public static StandardStashReader getInstance(URI stashRoot, AWSCredentialsProvider credentialsProvider,
                                                  ClientConfiguration s3Config) {
        AmazonS3 s3 = getS3Client(stashRoot, credentialsProvider, s3Config);
        return getInstance(stashRoot, s3);
    }

    public static StandardStashReader getInstance(URI stashRoot, AmazonS3 s3) {
        return new StandardStashReader(stashRoot, s3, REFRESH_LATEST_MS);
    }

    @VisibleForTesting
    StandardStashReader(URI stashRoot, AmazonS3 s3, long refreshLatestMs) {
        super(stashRoot, s3);

        Supplier<String> s3LatestRootSupplier = new Supplier<String>() {
            @Override
            public String get() {
                String latest = readLatestStash();
                return String.format("%s/%s", _rootPath, latest);
            }
        };

        if (refreshLatestMs > 0) {
            // Cache the latest stash directory and periodically recheck
            _latestRootSupplier = Suppliers.memoizeWithExpiration(s3LatestRootSupplier, refreshLatestMs, TimeUnit.MILLISECONDS);
        } else {
            _latestRootSupplier = s3LatestRootSupplier;
        }
    }

    /**
     * Gets the latest Stash at the time of the call unless it was locked by a prior call to {@link #lockToLatest()},
     * in which case it returns the latest at the time of that call.
     */
    @Override
    protected String getRootPath()
            throws StashNotAvailableException {
        if (_lockedLatest != null) {
            return _lockedLatest;
        }
        return _latestRootSupplier.get();
    }

    /**
     * Gets the latest Stash at the time of the call unless it was locked by a prior call to {@link #lockToLatest()},
     * in which case it returns the latest at the time of that call.
     */
    public String getLatest()
            throws StashNotAvailableException {
        String latestRootPath = getRootPath();
        return latestRootPath.substring(_rootPath.length() + 1);
    }

    /**
     * Returns the time the Stash returned by {@link #getLatest()} was created.
     */
    public Date getLatestCreationTime()
            throws StashNotAvailableException {
        return StashUtil.getStashCreationTime(getLatest());
    }

    public Date getStashCreationTime() throws ParseException {
        String root = getRootPath();
        String successFile = String.format("%s/%s", root, StashUtil.SUCCESS_FILE);
        return StashUtil.getStashCreationTimeStamp(readFirstLineFromS3File(_bucket, successFile));
    }

    /**
     * Internal method to fetch the "latest" file from S3 and return its contents.
     */
    private String readLatestStash() {
        return readFirstLineFromS3File(_bucket, String.format("%s/%s", _rootPath, StashUtil.LATEST_FILE));
    }

    private String readFirstLineFromS3File(String bucket, String path) {
        S3Object s3Object;
        try {
            // In the unlikely case that someone replaced the Stash file with a malicious file intended to cause
            // a memory overrun restrict the file contents fetched to a reasonably high limit.
            s3Object = _s3.getObject(new GetObjectRequest(bucket, path).withRange(0, 2048));
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new StashNotAvailableException();
            }
            throw e;
        }

        try (BufferedReader in = new BufferedReader(new InputStreamReader(s3Object.getObjectContent(), Charsets.UTF_8))) {
            return in.readLine();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * By default any time stash contents are queried the reader always tries to use the latest Stash available.  For long
     * operations the caller may with to have a consistent view across multiple tables.  By calling this method all
     * future Stash calls will use the whatever the latest Stash was at the time this method was called.  This can be
     * then be disabled by subsequently calling {@link #unlock()}.
     */
    public String lockToLatest() {
        _lockedLatest = _latestRootSupplier.get();
        return _lockedLatest;
    }

    /**
     * This method is like {@link #lockToLatest()} except that the caller requests a specific Stash time.
     * @throws StashNotAvailableException Thrown if no Stash is available for the given time
     */
    public void lockToStashCreatedAt(Date creationTime)
            throws StashNotAvailableException {
        String stashDirectory = StashUtil.getStashDirectoryForCreationTime(creationTime);
        // The following call will raise an AmazonS3Exception if the file cannot be read
        try (S3Object s3Object = _s3.getObject(_bucket, String.format("%s/%s/%s", _rootPath, stashDirectory, StashUtil.SUCCESS_FILE))) {
            _lockedLatest = String.format("%s/%s", _rootPath, stashDirectory);
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode() ||
                    // The following conditions indicate the file has already been moved to Glacier
                    (e.getStatusCode() == Response.Status.FORBIDDEN.getStatusCode() && "InvalidObjectState".equals(e.getErrorCode()))) {
                throw new StashNotAvailableException();
            }
            throw e;
        } catch (IOException e) {
            // Shouldn't happen since the file is never actually read
        }
    }

    /**
     * Returns a new StashReader that is locked to the same stash time the instance is currently using.  Future calls to
     * lock or unlock the stash time on this instance will not affect the returned instance.
     */
    public StashReader getLockedView() {
        return new FixedStashReader(URI.create(String.format("s3://%s/%s", _bucket, getRootPath())), _s3);
    }

    /**
     * If the Stash had been locked using {@link #lockToLatest()} or {@link #lockToStashCreatedAt(java.util.Date)}
     * this method unlocks it.  All subsequent Stash calls will attempt to use the latest Stash available.
     */
    public void unlock() {
        _lockedLatest = null;
    }
}
