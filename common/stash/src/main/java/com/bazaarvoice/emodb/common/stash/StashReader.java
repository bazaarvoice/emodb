package com.bazaarvoice.emodb.common.stash;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.bazaarvoice.emodb.sor.api.StashNotAvailableException;
import com.bazaarvoice.emodb.sor.api.TableNotStashedException;
import com.bazaarvoice.emodb.streaming.AbstractSpliterator;
import com.bazaarvoice.emodb.streaming.SpliteratorIterator;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Spliterator;
import java.util.stream.StreamSupport;

/**
 * Provides basic access to Stashed tables and content.
 */
abstract public class StashReader {

    protected final AmazonS3 _s3;
    protected final String _bucket;
    protected final String _rootPath;

    protected StashReader(URI stashRoot, AmazonS3 s3) {
        if (stashRoot == null) {
            throw new NullPointerException("stashRoot is required");
        }
        if (s3 == null) {
            throw new NullPointerException("s3 is required");
        }
        _s3 = s3;
        _bucket = stashRoot.getHost();

        String path = stashRoot.getPath();
        if (path == null) {
            path = "";
        } else if (path.startsWith("/")) {
            // S3 paths don't have a leading slash
            path = path.substring(1);
        }
        _rootPath = path;
    }

    /**
     * Returns the root path; immediate subdirectories of this path are tables.  This must always be  prefixed by
     * _rootPath:
     *
     * <code>
     *     assert getRootPath().startsWith(_rootPath);
     * </code>
     */
    abstract protected String getRootPath();

    /**
     * Utility method to get the S3 client for a credentials provider.
     */
    protected static AmazonS3 getS3Client(URI stashRoot, AWSCredentialsProvider credentialsProvider) {
        return getS3Client(stashRoot, credentialsProvider, null);
    }

    protected static AmazonS3 getS3Client(URI stashRoot, final AWSCredentialsProvider credentialsProvider,
                                          final @Nullable ClientConfiguration s3Config) {
        final String bucket = stashRoot.getHost();

        // If the bucket is a well-known Stash bucket then the region for the bucket is known in advance.
        // Otherwise return a proxy which lazily looks up the bucket on the first call.

        return StashUtil.getRegionForBucket(bucket)
                .map(region -> createS3ClientForRegion(region, credentialsProvider, s3Config))
                .orElseGet(() -> (AmazonS3) Proxy.newProxyInstance(
                        AmazonS3.class.getClassLoader(), new Class<?>[] { AmazonS3.class },
                        new InvocationHandler() {
                            private AmazonS3 _resolvedClient;
                            
                            @Override
                            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                                return method.invoke(resolvedClient(), args);
                            }

                            private AmazonS3 resolvedClient() {
                                if (_resolvedClient == null) {
                                    String endPoint = determineEndpointForBucket(bucket, credentialsProvider, s3Config, stashRoot.getPath());
                                    _resolvedClient = createS3ClientForEndpoint(endPoint, credentialsProvider, s3Config);
                                }
                                return _resolvedClient;
                            }
                }));
    }

    private static AmazonS3 createS3ClientForRegion(String region, AWSCredentialsProvider credentialsProvider,
                                                    @Nullable ClientConfiguration s3Config) {
        return createS3ClientForEndpoint(String.format("s3.%s.amazonaws.com", region), credentialsProvider, s3Config);
    }

    private static AmazonS3 createS3ClientForEndpoint(String endPoint, AWSCredentialsProvider credentialsProvider,
                                                      @Nullable ClientConfiguration s3Config) {
        AmazonS3 s3;
        if (s3Config == null) {
            s3 = new AmazonS3Client(credentialsProvider);
        } else {
            s3 = new AmazonS3Client(credentialsProvider, s3Config);
        }
        s3.setEndpoint(endPoint);
        return s3;
    }

    private static String determineEndpointForBucket(String bucket, AWSCredentialsProvider credentialsProvider,
                                                     @Nullable ClientConfiguration s3Config, String rootPath) {

        // Guess us-east-1.  If wrong AWS will return a redirect with the correct endpoint
        AmazonS3 s3 = createS3ClientForRegion(Regions.US_EAST_1.getName(), credentialsProvider, s3Config);
        if (rootPath.startsWith("/")) {
            rootPath = rootPath.substring(1);
        }
        if (!rootPath.endsWith("/")) {
            rootPath = rootPath + "/";
        }

        try {
            // Any request will work but presumably the client has list access for stash so perform a list.
            s3.listObjects(new ListObjectsRequest()
                    .withBucketName(bucket)
                    .withPrefix(rootPath)
                    .withDelimiter("/")
                    .withMaxKeys(1));

            // If this didn't error out then the presumed us-east-1 region was correct
            return  "s3.us-east-1.amazonaws.com";
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 301 /* MOVED_PERMANENTLY */) {
                String endPoint = e.getAdditionalDetails().get("Endpoint");
                // The end point is prefixed with the bucket name, so strip it
                return endPoint.substring(bucket.length() + 1);
            }

            throw e;
        }
    }

    /**
     * Gets all tables available in this stash.
     */
    public Iterator<StashTable> listTables() {
        final String root = getRootPath();
        final String prefix = String.format("%s/", root);

        return new SpliteratorIterator<StashTable>() {
            Iterator<String> _commonPrefixes = Collections.emptyIterator();
            String _marker = null;
            boolean _truncated = true;

            @Override
            protected Spliterator<StashTable> getSpliterator() {
                return new AbstractSpliterator<StashTable>() {
                    @Override
                    protected StashTable computeNext() {
                        String dir = null;

                        while (dir == null) {
                            if (_commonPrefixes.hasNext()) {
                                dir = _commonPrefixes.next();
                                if (dir.isEmpty()) {
                                    // Ignore the empty directory if it comes back
                                    dir = null;
                                } else {
                                    // Strip the prefix and trailing "/"
                                    dir = dir.substring(prefix.length(), dir.length()-1);
                                }
                            } else if (_truncated) {
                                ObjectListing response = _s3.listObjects(new ListObjectsRequest()
                                        .withBucketName(_bucket)
                                        .withPrefix(prefix)
                                        .withDelimiter("/")
                                        .withMarker(_marker)
                                        .withMaxKeys(1000));

                                _commonPrefixes = response.getCommonPrefixes().iterator();
                                _marker = response.getNextMarker();
                                _truncated = response.isTruncated();
                            } else {
                                return endOfStream();
                            }
                        }

                        String tablePrefix = prefix + dir + "/";
                        String tableName = StashUtil.decodeStashTable(dir);
                        return new StashTable(_bucket, tablePrefix, tableName);
                    }
                };
            }
        };
    }

    /**
     * Gets the metadata for all tables in this stash.  This is a heavier operation that just {@link #listTables()}
     * since it also returns full file details for the entire Stash instead of just table names.
     */
    public Iterator<StashTableMetadata> listTableMetadata() {
        final String root = getRootPath();
        final String prefix = String.format("%s/", root);
        final int prefixLength = prefix.length();

        return new SpliteratorIterator<StashTableMetadata>() {
            @Override
            protected Spliterator<StashTableMetadata> getSpliterator() {
                return new AbstractSpliterator<StashTableMetadata>() {
                    ListIterator<S3ObjectSummary> _listResponse = Collections.emptyListIterator();
                    String _marker = null;
                    boolean _truncated = true;

                    @Override
                    protected StashTableMetadata computeNext() {
                        String tableDir = null;
                        List<StashFileMetadata> files = new ArrayList<>(16);
                        boolean allFilesRead = false;

                        while (!allFilesRead) {
                            if (_listResponse.hasNext()) {
                                // Peek at the next record but don't consume it until we verify it's part of the same table
                                S3ObjectSummary s3File = _listResponse.next();
                                _listResponse.previous();
                                String key = s3File.getKey();

                                // Don't include the _SUCCESS file or any other stray files we may find
                                String[] parentDirAndFile = key.substring(prefixLength).split("/");
                                if (parentDirAndFile.length != 2) {
                                    // Consume and skip this row
                                    _listResponse.next();
                                } else {
                                    String parentDir = parentDirAndFile[0];
                                    if (tableDir == null) {
                                        tableDir = parentDir;
                                    }

                                    if (!parentDir.equals(tableDir)) {
                                        allFilesRead = true;
                                    } else {
                                        // Record is part of this table; consume it now
                                        _listResponse.next();
                                        files.add(new StashFileMetadata(_bucket, key, s3File.getSize()));
                                    }
                                }
                            } else if (_truncated) {
                                ObjectListing response = _s3.listObjects(new ListObjectsRequest()
                                        .withBucketName(_bucket)
                                        .withPrefix(prefix)
                                        .withMarker(_marker)
                                        .withMaxKeys(1000));

                                _listResponse = response.getObjectSummaries().listIterator();
                                _marker = response.getNextMarker();
                                _truncated = response.isTruncated();
                            } else {
                                allFilesRead = true;
                            }
                        }

                        if (tableDir == null) {
                            // No files read this iteration means all files have been read
                            return endOfStream();
                        }

                        String tablePrefix = prefix + tableDir + "/";
                        String tableName = StashUtil.decodeStashTable(tableDir);
                        return new StashTableMetadata(_bucket, tablePrefix, tableName, files);
                    }
                };
            }
        };
    }

    /**
     * Gets the metadata for a single table in this stash.  This is similar to getting the splits for the table
     * except that it exposes lower level information about the underlying S3 files.  For clients who will use
     * their own system for reading the files from S3, such as source files for a map-reduce job, this method provides
     * the necessary information.  For simply iterating over the stash contents using either {@link #scan(String)}
     * or {@link #getSplits(String)} in conjunction with {@link #getSplit(StashSplit)} is preferred.
     */
    public StashTableMetadata getTableMetadata(String table)
            throws StashNotAvailableException, TableNotStashedException {
        List<StashFileMetadata> files = new ArrayList<>();

        Iterator<S3ObjectSummary> objectSummaries = getS3ObjectSummariesForTable(table);
        while (objectSummaries.hasNext()) {
            S3ObjectSummary objectSummary = objectSummaries.next();
            files.add(new StashFileMetadata(_bucket, objectSummary.getKey(), objectSummary.getSize()));
        }

        // Get the prefix arbitrarily from the first file.
        String prefix = files.get(0).getKey();
        prefix = prefix.substring(0, prefix.lastIndexOf('/') + 1);

        return new StashTableMetadata(_bucket, prefix, table, files);
    }

    public boolean getTableExists(String table) {
        return getS3ObjectSummariesForTable(table).hasNext();
    }

    /**
     * Get the splits for a record stored in stash.  Each split corresponds to a file in the Stash table's directory.
     */
    public List<StashSplit> getSplits(String table)
            throws StashNotAvailableException, TableNotStashedException {
        List<StashSplit> splits = new ArrayList<>();

        Iterator<S3ObjectSummary> objectSummaries = getS3ObjectSummariesForTable(table);
        while (objectSummaries.hasNext()) {
            S3ObjectSummary objectSummary = objectSummaries.next();
            String key = objectSummary.getKey();
            // Strip the common root path prefix from the split since it is constant.
            splits.add(new StashSplit(table, key.substring(_rootPath.length() + 1), objectSummary.getSize()));
        }

        return splits;
    }

    /**
     * Gets an iterator over the contents of a split returned by {@link #getSplits(String)}.  If possible the caller
     * should call {@link com.bazaarvoice.emodb.common.stash.StashRowIterator#close()} when done with the iterator
     * to immediately free any S3 connections.
     */
    public StashRowIterator getSplit(final StashSplit split) {
        return new StashSplitIterator(_s3, _bucket, getSplitKey(split));
    }

    /**
     * Gets an iterator over the entire contents of a Stash table.  If possible the caller should call
     * {@link com.bazaarvoice.emodb.common.stash.StashRowIterator#close()} when done with the iterator
     * to immediately free any S3 connections.
     */
    public StashRowIterator scan(String table)
            throws StashNotAvailableException, TableNotStashedException {
        List<StashSplit> splits = getSplits(table);
        return new StashScanIterator(_s3, _bucket, _rootPath, splits);
    }

    private String getSplitKey(StashSplit split) {
        // The key in the split has the root removed, so we need to put it back.
        return String.format("%s/%s", _rootPath, split.getKey());
    }

    // The following methods are fairly low level and do not typically need to be accessed

    public InputStream getRawSplit(StashSplit split) {
        return new RestartingS3InputStream(_s3, _bucket, getSplitKey(split));
    }

    public InputStream getRawSplitPart(StashSplit split, @Nullable Long fromInclusive, @Nullable Long toExclusive) {
        return new RestartingS3InputStream(_s3, _bucket, getSplitKey(split), fromInclusive, toExclusive);
    }

    private String getPrefix(String table) {
        String root = getRootPath();
        return String.format("%s/%s/", root, StashUtil.encodeStashTable(table));
    }

    private Iterator<S3ObjectSummary> getS3ObjectSummariesForTable(String table)
            throws TableNotStashedException {
        String prefix = getPrefix(table);
        Iterator<S3ObjectSummary> summaryIterator = getS3ObjectSummaries(prefix);

        if (!summaryIterator.hasNext()) {
            throw new TableNotStashedException(table);
        }

        return summaryIterator;
    }

    private Iterator<S3ObjectSummary> getS3ObjectSummaries(final String prefix) {
        final int prefixLength = prefix.length();

        Spliterator<S3ObjectSummary> allSummaries = new AbstractSpliterator<S3ObjectSummary>() {
            private String _marker = null;
            private Iterator<S3ObjectSummary> _summaries = null;

            @Override
            protected S3ObjectSummary computeNext() {
                if (_summaries == null || (!_summaries.hasNext() && _marker != null)) {
                    ObjectListing response = _s3.listObjects(new ListObjectsRequest()
                            .withBucketName(_bucket)
                            .withPrefix(prefix)
                            .withDelimiter("/")
                            .withMarker(_marker)
                            .withMaxKeys(1000));
                    _marker = response.getNextMarker();
                    _summaries = response.getObjectSummaries().iterator();
                }
                if (_summaries.hasNext()) {
                    return _summaries.next();
                }
                return endOfStream();
            }
        };

        return StreamSupport.stream(allSummaries, false)
                // Sometimes the prefix itself can come back as a result.  Filter that entry out.
                .filter(summary -> summary.getKey().length() > prefixLength)
                .iterator();
    }
}
