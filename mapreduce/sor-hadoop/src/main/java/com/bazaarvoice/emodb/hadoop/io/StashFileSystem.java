package com.bazaarvoice.emodb.hadoop.io;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.bazaarvoice.emodb.common.stash.FixedStashReader;
import com.bazaarvoice.emodb.common.stash.StandardStashReader;
import com.bazaarvoice.emodb.common.stash.StashReader;
import com.bazaarvoice.emodb.common.stash.StashRowIterator;
import com.bazaarvoice.emodb.common.stash.StashSplit;
import com.bazaarvoice.emodb.common.stash.StashTable;
import com.bazaarvoice.emodb.hadoop.ConfigurationParameters;
import com.bazaarvoice.emodb.sor.api.TableNotStashedException;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import javax.ws.rs.core.UriBuilder;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getEmptySplitFileName;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getEmptySplitRecordReader;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getRootFileStatus;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getSplitFileStatus;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getSplitName;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getSplitPath;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getTableFileStatus;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getTableName;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.isEmptySplit;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * FileSystem implementation backed by EmoStash.  After initializing the root URI immediate S3 subdirectories
 * are tables and any leaf files under the tables are splits.
 */
public class StashFileSystem extends FileSystem implements EmoInputSplittable {

    private static final Regions DEFAULT_REGION = Regions.US_EAST_1;
    private static final int BLOCK_SIZE = 10 * 1024 * 1024;

    private static final String SPLIT_PREFIX = "S_";
    private static final Pattern SPLIT_PATTERN = Pattern.compile("^S_(?<split>[0-9a-zA-Z_\\-]+)\\.gz$");

    private URI _uri;
    private Path _rootPath;
    private AmazonS3Client _s3;
    private StashReader _stashReader;
    private final AtomicInteger _stashReaderRefCount = new AtomicInteger(0);
    private final AtomicBoolean _closed = new AtomicBoolean(false);

    public StashFileSystem() {
        // empty
    }

    @Override
    public String getScheme() {
        return "emostash";
    }

    @Override
    public void initialize(URI location, Configuration conf)
            throws IOException {
        // The location is either a table or a split.  Get the root path depending on which it is.

        // Strip the trailing slash if present
        String locationPath = Objects.firstNonNull(location.getPath(), "/");
        if (locationPath.length() > 0 && locationPath.endsWith("/")) {
            locationPath = locationPath.substring(0, locationPath.length() - 1);
        }

        // Get the parent directory
        String basePath = location.getPath();
        int lastSlash = locationPath.lastIndexOf('/');
        // If it's a split go to the parent's parent.
        boolean isSplit = isSplitFile(basePath.substring(lastSlash + 1));
        for (int i=0; i < (isSplit ? 2 : 1); i++) {
            basePath = lastSlash != 0 ? basePath.substring(0, lastSlash) : "/";
            lastSlash = basePath.lastIndexOf('/');
        }

        _uri = UriBuilder.fromUri(location).replacePath(basePath).build();
        _rootPath = new Path(_uri);

        _s3 = createS3Client(conf);
        addS3ClientReference("instance");

        StashLocation stashLocation = LocationUtil.getStashLocation(_uri);

        /**
         * Some locations are fixed in that the root directory directly contains the table directories.
         * Other locations, such as "emostash://ci.us", are dynamic in that the actual root directory is
         * in a directory beneath the root directory.  Which subdirectory to use is determined by reading the
         * content of a signal file called "_LATEST".  This is handled by the StandardStashReader.
         *
         */
        boolean useLatestDirectory = stashLocation.isUseLatestDirectory();

        if (useLatestDirectory) {
            _stashReader = StandardStashReader.getInstance(stashLocation.getUri(), _s3);
        } else {
            _stashReader = FixedStashReader.getInstance(stashLocation.getUri(), _s3);
        }

        super.initialize(_uri, conf);
    }

    private AmazonS3Client createS3Client(Configuration conf) {
        AWSCredentialsProvider credentials;

        String accessKey = conf.get(ConfigurationParameters.ACCESS_KEY_PARAM);
        String secretKey = conf.get(ConfigurationParameters.SECRET_KEY_PARAM);
        if (accessKey != null || secretKey != null) {
            // Keys explicitly configured
            checkArgument(accessKey != null && secretKey != null, "Access and secret keys must both be provided");
            credentials = new StaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        } else {
            // Use the default provider chain
            credentials = new DefaultAWSCredentialsProviderChain();
        }

        String regionParam = conf.get(ConfigurationParameters.REGION_PARAM);
        Region region = Region.getRegion(regionParam != null ? Regions.fromName(regionParam) : DEFAULT_REGION);

        AmazonS3Client s3 = new AmazonS3Client(credentials);
        s3.setRegion(region);
        return s3;
    }

    @Override
    public URI getUri() {
        return _uri;
    }

    @Override
    public void close()
            throws IOException {
        if (_closed.compareAndSet(false, true)) {
            try {
                super.close();
            } finally {
                maybeCloseS3Client("instance");
            }
        }
    }

    private void addS3ClientReference(String reason) {
        _stashReaderRefCount.incrementAndGet();
        LOG.debug("Incremented S3 client reference count for " + reason);
    }
    /**
     * Closes the S3 client if it is completely dereferenced.  This is necessary because Hadoop may close the file
     * system while a stream returned by {@link #open(org.apache.hadoop.fs.Path, int)} is still open.
     */
    private void maybeCloseS3Client(String reason) {
        LOG.debug("Decremented S3 client reference count for " + reason);

        if (_stashReaderRefCount.decrementAndGet() == 0) {
            LOG.debug("Closing S3 client for StashFileSystem at " + _uri);
            _s3.shutdown();
        }
    }

    private boolean isSplitFile(String fileName) {
        return SPLIT_PATTERN.matcher(fileName).matches() || isEmptySplit(fileName);
    }

    private String toSplitFile(StashSplit stashSplit) {
        // Need to convert each split to a file with a unique recognizable pattern.  We also need to preserve the
        // original file's extension so that Hadoop will honor the underlying compression scheme.
        String extension = Files.getFileExtension(stashSplit.getFile());
        return String.format("%s%s.%s", SPLIT_PREFIX, stashSplit, extension);
    }

    private StashSplit fromSplitFile(String fileName) {
        Matcher matcher = SPLIT_PATTERN.matcher(fileName);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("File name does not represent a split file: " + fileName);
        }
        String stashSplitString = matcher.group("split");
        return StashSplit.fromString(stashSplitString);
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException {
        // Only valid if the path is root or a table
        if (getSplitName(_rootPath, path) != null) {
            throw new IOException("Cannot list a split");
        }

        ImmutableList.Builder<FileStatus> resultsBuilder = ImmutableList.builder();
        String table = getTableName(_rootPath, path);

        if (table == null) {
            // Return the list of tables
            Iterator<StashTable> tableIterator = _stashReader.listTables();
            while (tableIterator.hasNext()) {
                StashTable stashTable = tableIterator.next();
                resultsBuilder.add(getTableFileStatus(_rootPath, stashTable.getTableName()));
            }
        } else {
            // Return the list of splits
            try {
                List<StashSplit> splits = _stashReader.getSplits(table);
                for (StashSplit split : splits) {
                    resultsBuilder.add(getSplitFileStatus(_rootPath, table, toSplitFile(split), split.getSize(), BLOCK_SIZE));
                }
            } catch (TableNotStashedException e) {
                // Ok, table is not in stash; therefore there are no splits
            }
        }

        FileStatus[] results = FluentIterable.from(resultsBuilder.build()).toArray(FileStatus.class);

        if (results.length == 0 && table != null) {
            // Do not return an empty list of splits, return a single empty split
            results = new FileStatus[] { getSplitFileStatus(_rootPath, table, getEmptySplitFileName(), 1, BLOCK_SIZE) };
        }

        return results;
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException {
        if (path.equals(_rootPath)) {
            return getRootFileStatus(_rootPath);
        }

        String table = getTableName(_rootPath, path);
        String split = getSplitName(_rootPath, path);

        if (split == null) {
            // This is a table.  Since S3 doesn't have a concept of empty directories assume all tables exist
            return getTableFileStatus(_rootPath, table);
        }

        // This is a split
        if (isEmptySplit(path)) {
            // Always return that the empty split exists
            return getSplitFileStatus(_rootPath, table, split, 1, BLOCK_SIZE);
        } else {
            StashSplit stashSplit = fromSplitFile(split);
            return getSplitFileStatus(_rootPath, table, split, stashSplit.getSize(), BLOCK_SIZE);
        }
    }

    @Override
    public List<SplitPath> getInputSplits(Configuration config, Path path, int splitSize)
            throws IOException {
        ImmutableList.Builder<SplitPath> splits = ImmutableList.builder();

        RemoteIterator<LocatedFileStatus> files = listFiles(path, false);
        if (!files.hasNext()) {
            // No splits.  Don't return nothing, return a single empty split
            String table = getTableName(_rootPath, path);
            return ImmutableList.of(new SplitPath(getSplitPath(_rootPath, table, getEmptySplitFileName()), 1));
        }

        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            splits.add(new SplitPath(file.getPath(), file.getLen()));
        }

        return splits.build();
    }

    @Override
    public BaseRecordReader getBaseRecordReader(Configuration config, Path path, int splitSize)
            throws IOException {
        String split = getSplitName(_rootPath, path);
        if (isEmptySplit(split)) {
            return getEmptySplitRecordReader();
        }

        final String reason = "record reader for split " + path;
        final StashSplit stashSplit = fromSplitFile(split);
        // Increment the s3 client reference count so it stays open at least until the returned reader is closed.
        addS3ClientReference(reason);

        return new BaseRecordReader(splitSize) {
            private StashRowIterator _iterator;

            @Override
            protected Iterator<Map<String, Object>> getRowIterator() throws IOException {
                _iterator = _stashReader.getSplit(stashSplit);
                return _iterator;
            }

            @Override
            protected void closeOnce() throws IOException {
                try {
                    Closeables.close(_iterator, false);
                } finally {
                    maybeCloseS3Client(reason);
                }
            }
        };
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize)
            throws IOException {
        String split = getSplitName(_rootPath, path);
        if (split == null) {
            throw new IOException("File is not a split");
        }

        if (isEmptySplit(split)) {
            return new EmptyFSDataInputStream();
        }

        StashSplit stashSplit = fromSplitFile(split);

        // The input stream requires the S3 client to remain open, so increment the reference counter
        String reason = "input stream for split " + path;
        addS3ClientReference(reason);
        return new FSDataInputStream(new StashInputStream(stashSplit, reason, bufferSize));
    }

    /**
     * InputStream implementation which satisfies the Hadoop required interfaces and uses the S3 connection
     * from the parent class.
     */
    private class StashInputStream extends InputStream implements Seekable, PositionedReadable {
        private final StashSplit _stashSplit;
        private final String _s3ClientCloseReason;
        private final int _bufferSize;
        private final AtomicBoolean _closed = new AtomicBoolean(false);
        private BufferedInputStream _in;
        private long _seekStart = 0;
        private long _pos;

        private StashInputStream(StashSplit stashSplit, String s3ClientCloseReason, int bufferSize) {
            _stashSplit = stashSplit;
            _s3ClientCloseReason = s3ClientCloseReason;
            _bufferSize = bufferSize;
        }

        /**
         * Lazily open the input stream if it has not already been opened.
         */
        private InputStream getInputStream() {
            if (_in == null) {
                InputStream stashIn = _stashReader.getRawSplitPart(_stashSplit, Range.closedOpen(_seekStart, _stashSplit.getSize()));
                _in = new BufferedInputStream(stashIn, _bufferSize);
            }
            return _in;
        }

        @Override
        public int read()
                throws IOException {
            int b = getInputStream().read();
            if (b != -1) {
                _pos += 1;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len)
                throws IOException {
            int bytesRead = getInputStream().read(b, off, len);
            if (bytesRead != -1) {
                _pos += bytesRead;
            }
            return bytesRead;
        }

        @Override
        public void close()
                throws IOException {
            if (_closed.compareAndSet(false, true)) {
                try {
                    Closeables.close(_in, false);
                } finally {
                    maybeCloseS3Client(_s3ClientCloseReason);
                }
            }
        }

        @Override
        protected void finalize()
                throws Throwable {
            super.finalize();
            close();
        }

        @Override
        public void seek(long pos)
                throws IOException {
            if (_in != null) {
                _in.close();
                _in = null;
            }
            _seekStart = pos;
        }

        @Override
        public long getPos()
                throws IOException {
            return _pos;
        }

        @Override
        public boolean seekToNewSource(long targetPos)
                throws IOException {
            // This isn't supported; return false to inform the caller.
            return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length)
                throws IOException {
            try (InputStream in = _stashReader.getRawSplitPart(_stashSplit, Range.closedOpen(position, position + length))) {
                return ByteStreams.read(in, buffer, offset, length);
            }
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length)
                throws IOException {
            try (InputStream in = _stashReader.getRawSplitPart(_stashSplit, Range.closedOpen(position, position + length))) {
                ByteStreams.readFully(in, buffer, offset, length);
            }
        }

        @Override
        public void readFully(long position, byte[] buffer)
                throws IOException {
            readFully(position, buffer, 0, buffer.length);
        }
    }

    // All remaining FileSystem operations are not supported and will throw IOExceptions.

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException {
        throw new IOException("Create not supported for StashFileSystem: " + f);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
            throws IOException {
        throw new IOException("Append not supported for StashFileSystem: " + f);
    }

    @Override
    public boolean rename(Path src, Path dst)
            throws IOException {
        throw new IOException("Rename not supported for StashFileSystem: " + src);
    }

    @Override
    public boolean delete(Path f, boolean recursive)
            throws IOException {
        throw new IOException("Delete not supported for StashFileSystem: " + f);
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        throw new UnsupportedOperationException("Working directories not supported for StashFileSystem");
    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(_uri);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission)
            throws IOException {
        throw new IOException("Making directories not supported for StashFileSystem: " + f);
    }
}
