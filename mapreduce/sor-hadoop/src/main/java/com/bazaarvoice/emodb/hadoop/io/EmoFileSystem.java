package com.bazaarvoice.emodb.hadoop.io;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.hadoop.ConfigurationParameters;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.Table;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.client.DataStoreStreaming;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Closeables;
import com.google.common.io.Closer;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.util.ByteBufferOutputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.Progressable;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getEmptySplitFileName;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getEmptySplitRecordReader;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getRootFileStatus;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getSplitFileStatus;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getSplitName;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getSplitPath;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getTableFileStatus;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.getTableName;
import static com.bazaarvoice.emodb.hadoop.io.FileSystemUtil.isEmptySplit;

/**
 * FileSystem implementation backed by EmoDB.  The file system consists of the following paths:
 *
 * <dl>
 *     <dd>/</dd>
 *     <dt>The root directory.  Subdirectories are tables.</dt>
 *
 *     <dd>/table_name</dd>
 *     <dt>A table.  This is is represented as a directory in the file system.</dt>
 *
 *     <dd>/table_name/split</dd>
 *     <dt>A split.  A split is a file in the file system.</dt>
 * </dl>
 *
 * Note the the status for splits can be gotten from the file system but they cannot be opened.  The caller must
 * use one of the EmoInputFormat classes to actually read a split file's contents.
 */
public class EmoFileSystem extends FileSystem implements EmoInputSplittable {
    private URI _uri;
    private String _apiKey;
    private Path _rootPath;
    private int _splitSize;
    private MetricRegistry _metricRegistry;

    public EmoFileSystem() {
        // Since this class should be used outside of the context of a Dropwizard server, we simply create our own
        // MetricRegistry instance instead of relying on the environment's.
        _metricRegistry = new MetricRegistry();
    }

    @Override
    public String getScheme() {
        return "emodb";
    }

    @Override
    public void initialize(URI location, Configuration conf)
            throws IOException {
        super.initialize(location, conf);

        Optional<String> explicitZkConnectionString = LocationUtil.getZkConnectionStringOverride(location);
        Optional<List<String>> explicitHosts = LocationUtil.getHostOverride(location);

        // Set the ZooKeeper connection string if it is present in the config and not explicitly set in the location
        if (!explicitZkConnectionString.isPresent()) {
            String zkConnectionString = conf.get(ConfigurationParameters.ZOOKEEPER_CONNECTION_STRING_PARAM);
            if (zkConnectionString != null) {
                location = LocationUtil.setZkConnectionStringOverride(location, zkConnectionString);
            }
        }

        // Set the hosts if they is present in the config and not explicitly set in the location
        if (!explicitHosts.isPresent()) {
            String hosts = conf.get(ConfigurationParameters.HOSTS_PARAM);
            if (hosts != null) {
                location = LocationUtil.setHostsOverride(location, hosts.split(","));
            }
        }

        _uri = UriBuilder.fromUri(location).replacePath("/").build();
        _apiKey = conf.get(ConfigurationParameters.EMO_API_KEY);
        _rootPath = new Path(_uri);
        _splitSize = BaseInputFormat.getSplitSize(conf);

    }

    @Override
    public URI getUri() {
        return _uri;
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException {
        if (path.equals(_rootPath)) {
            // Root path.  List all tables as subdirectories.
            try (CloseableDataStore dataStore = HadoopDataStoreManager.getInstance().getDataStore(_uri, _apiKey, _metricRegistry)) {
                return FluentIterable
                        .from(DataStoreStreaming.listTables(dataStore))
                        .transform(new Function<Table, FileStatus>() {
                            @Override
                            public FileStatus apply(Table table) {
                                return getTableFileStatus(_rootPath, table.getName());
                            }
                        })
                        .toArray(FileStatus.class);
            }
        }

        // Other than root only tables can be listed
        if (getSplitName(_rootPath, path) != null) {
            throw new IOException("Cannot list a split");
        }

        final String table = getTableName(_rootPath, path);

        // Simulate a file for each split
        Collection<String> splits = getSplitsFromDataStore(table);

        return FluentIterable.from(splits)
                .transform(new Function<String, FileStatus>() {
                    @Override
                    public FileStatus apply(String split) {
                        // Split length has no meaning, use max value to make it appear large since actual size is unknown
                        return getSplitFileStatus(_rootPath, table, split + ".gz", Long.MAX_VALUE, 1024);
                    }
                })
                .toArray(FileStatus.class);
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
            // This is a table.  Even if the table doesn't exist still return a value.
            return getTableFileStatus(_rootPath, table);
        }

        // This is a split.  As before we're using max long for the split size.
        return getSplitFileStatus(_rootPath, table, splitAsGzipFile(split), Long.MAX_VALUE, 1024);
    }

    @Override
    public List<SplitPath> getInputSplits(Configuration config, Path path, int splitSize)
            throws IOException {
        String table = getTableName(_rootPath, path);
        ImmutableList.Builder<SplitPath> splits = ImmutableList.builder();

        Collection<String> sourceSplits = getSplitsFromDataStore(table);
        for (String split : sourceSplits) {
            // Length is undefined and unused, use 1 for a simple positive value
            splits.add(new SplitPath(getSplitPath(_rootPath, table, splitAsGzipFile(split)), 1));
        }
        return splits.build();
    }

    private Collection<String> getSplitsFromDataStore(String table) {
        try (CloseableDataStore dataStore = HadoopDataStoreManager.getInstance().getDataStore(_uri, _apiKey, _metricRegistry)) {
            return dataStore.getSplits(table, _splitSize);
        } catch (Exception e) {
            // Return an empty collection of splits if the table does not exist
            if (Iterables.any(Throwables.getCausalChain(e), Predicates.instanceOf(UnknownTableException.class))) {
                return ImmutableList.of(getEmptySplitFileName());
            }
            throw Throwables.propagate(e);
        }
    }

    @Override
    public BaseRecordReader getBaseRecordReader(Configuration config, Path path, int splitSize)
            throws IOException {
        if (isEmptySplit(path)) {
            return getEmptySplitRecordReader();
        }

        final String table = getTableName(_rootPath, path);
        final String splitFile = getSplitName(_rootPath, path);
        final String split = splitNameWithoutGzipExtension(splitFile);
        final URI location = LocationUtil.toLocation(_uri, table);

        return new BaseRecordReader(splitSize) {
            private CloseableDataStore _dataStore;

            @Override
            protected Iterator<Map<String, Object>> getRowIterator()
                    throws IOException {
                try {
                    // Get the DataStore and begin streaming the split's rows.
                    _dataStore = HadoopDataStoreManager.getInstance().getDataStore(location, _apiKey, _metricRegistry);
                    Iterable<Map<String, Object>> rows =
                            DataStoreStreaming.getSplit(_dataStore, table, split, false, ReadConsistency.STRONG);
                    return rows.iterator();
                } catch (Exception e) {
                    close();
                    Throwables.propagateIfPossible(e, IOException.class);
                    throw Throwables.propagate(e);
                }
            }

            @Override
            protected void closeOnce()
                    throws IOException {
                Closeables.close(_dataStore, false);
            }
        };
    }

    /**
     * When not using EmoInputFormat the default behavior for TextInputFormat is to attempt to split a file
     * unless it is compressed with an unsplittable codec.  Since data streamed from EmoDB is not backed by an
     * actual file normal file operations cannot be applied to it, such as splitting and seeking.  To trick Hadoop
     * into not splitting the file make each split appear to be gzipped.
     */
    private String splitAsGzipFile(String split) {
        return split + ".gz";
    }

    /**
     * Since we appended a gzip extension to the split file name we need to take it off to get the actual split.
     */
    private String splitNameWithoutGzipExtension(String split)
            throws IOException {
        if (split == null) {
            throw new IOException("Path is not a split");
        }
        if (split.endsWith(".gz")) {
            return split.substring(0, split.length() - 3);
        }
        return split;
    }

    /**
     * Opens a split for reading.  Note that the preferred and more efficient way to do this is by using an
     * EmoInputFormat.  However, if using a MapReduce framework which does not support custom input formats,
     * such as Presto, the splits can be opened directly using this method.
     */
    @Override
    public FSDataInputStream open(Path path, int bufferSize)
            throws IOException {
        String table = getTableName(_rootPath, path);
        String split = getSplitName(_rootPath, path);
        split = splitNameWithoutGzipExtension(split);
        return new FSDataInputStream(new EmoSplitInputStream(table, split));
    }

    /**
     * InputStream which streams a split as a text file with one row of EmoDB JSON per line.  Then, since we made
     * the split appear to be gzipped, we gzip the streamed content.
     */
    private class EmoSplitInputStream extends InputStream implements Seekable, PositionedReadable {
        // Data is written to _rawOut in a separate thread, gzipped, and read from _gzipIn in the calling thread
        private final CompressionOutputStream _rawOut;
        private final PipedInputStream _gzipIn;
        // Buffer to hold each row of JSON
        private ByteBuffer _buffer = ByteBuffer.allocate(5120);
        // Input and output streams for shared buffer access
        private ByteBufferOutputStream _out = new ByteBufferOutputStream(_buffer);
        private Iterator<Map<String, Object>> _rows;
        // Maintain a single closer which will close all Closeables used by this instance
        private final Closer _closer = Closer.create();
        private final Thread _bufferThread;
        // If an exception is thrown in the buffering thread it is recorded here
        private volatile IOException _inputException;
        // Semaphore to halt the buffering thread in the event the thread is closed prior to being fully processed
        private volatile boolean _closed = false;
        private int _pos = 0;

        private EmoSplitInputStream(String table, String split)
                throws IOException {
            if (isEmptySplit(split)) {
                _rows = Collections.emptyIterator();
            } else {
                // Get the DataStore and begin streaming the split's rows.
                CloseableDataStore dataStore = HadoopDataStoreManager.getInstance().getDataStore(_uri, _apiKey, _metricRegistry);
                _closer.register(dataStore);

                _rows = DataStoreStreaming.getSplit(dataStore, table, split, false, ReadConsistency.STRONG).iterator();
            }

            _buffer.clear();
            _buffer.limit(0);
            GzipCodec gzipCodec = new GzipCodec();
            gzipCodec.setConf(new Configuration());

            // Set up the pipes
            PipedOutputStream pipeRawToGzip = new PipedOutputStream();
            _gzipIn = new PipedInputStream(pipeRawToGzip, 10 * 1024 * 1024);
            _rawOut = gzipCodec.createOutputStream(pipeRawToGzip);
            _closer.register(_gzipIn);
            _closer.register(pipeRawToGzip);

            // Start the asynchronous buffering thread
            _bufferThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    streamAndCompressInput();
                }
            });
            _bufferThread.start();
        }

        /**
         * Read data from the original input stream and pipe it to the compressing stream until fully read.
         */
        private void streamAndCompressInput() {
            try {
                byte[] newline = "\n".getBytes(Charsets.UTF_8);
                while (!_closed && fetchNextRow()) {
                    _rawOut.write(_buffer.array(), 0, _buffer.limit());
                    _rawOut.write(newline);
                }
                _rawOut.close();
            } catch (Exception e) {
                try {
                    Closer closer = Closer.create();
                    closer.register(_rawOut);
                    closer.register(_gzipIn);
                    closer.close();
                } catch (IOException ignore) {
                    // Ignore exceptions closing, don't mask the original exception.
                }
                if (!_closed) {
                    _inputException = e instanceof IOException ? (IOException ) e : new IOException(e);
                }
            }
        }

        public boolean fetchNextRow()
                throws IOException {
            if (!_rows.hasNext()) {
                return false;
            }

            // TODO:  Essentially we're streaming a JSON array of objects, converting the objects to Java Maps,
            //        then converting the Maps back to JSON strings.  There's possible efficiency improvement if we
            //        don't use DataStore and call the split API directly with a custom JSON parser.  However,
            //        to take advantage of the established DataStore client this has not been done at this time.
            Map<String, Object> row = _rows.next();
            try {
                // Attempt to read the row into the existing byte buffer.
                _buffer.clear();
                JsonHelper.writeJson(_out, row);
                _buffer.flip();
            } catch (Exception e) {
                if (Iterables.tryFind(Throwables.getCausalChain(e), Predicates.instanceOf(BufferOverflowException.class)).isPresent()) {
                    // Buffer overflow.  Allocate a new buffer and try again.
                    byte[] content = JsonHelper.asUtf8Bytes(row);
                    _buffer = ByteBuffer.wrap(content);
                    _out = new ByteBufferOutputStream(_buffer);
                } else {
                    Throwables.propagateIfPossible(e, IOException.class);
                    throw new IOException("Failed to read next row", e);
                }
            }

            return true;
        }

        @Override
        public int read()
                throws IOException {
            if (_inputException != null) {
                throw _inputException;
            }
            _pos += 1;
            return _gzipIn.read();
        }

        @Override
        public int read(byte[] b, int off, int len)
                throws IOException {
            if (_inputException != null) {
                throw _inputException;
            }
            int bytesRead = _gzipIn.read(b, off, len);
            if (bytesRead != -1) {
                _pos += bytesRead;
            }
            return bytesRead;
        }

        @Override
        public long getPos()
                throws IOException {
            return _pos;
        }

        @Override
        public void close()
                throws IOException {
            if (!_closed) {
                _closed = true;
                _closer.close();
                if (_inputException != null) {
                    throw _inputException;
                }
            }
        }

        // The Hadoop API forces this InputStream to extend Seekable and PositionedReadable.  Since there is
        // no actual file backing the split's contents neither of these interfaces can be satisfied.  However,
        // because the file is gzipped and gzip files are not splittable they should never be called.

        @Override
        public int read(long position, byte[] buffer, int offset, int length)
                throws IOException {
            throw new IOException("EmoFileSystem does not support read(long, byte[], int, int)");
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length)
                throws IOException {
            throw new IOException("EmoFileSystem does not support readFully(long, byte[], int, int)");
        }

        @Override
        public void readFully(long position, byte[] buffer)
                throws IOException {
            throw new IOException("EmoFileSystem does not support readFully(long, byte[])");
        }

        @Override
        public void seek(long pos)
                throws IOException {
            if (pos != _pos) {
                throw new IOException("Cannot seek");
            }
        }

        @Override
        public boolean seekToNewSource(long targetPos)
                throws IOException {
            return false;
        }
    }

    // All remaining FileSystem operations are not supported and will throw exceptions.

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException {
        throw new IOException("Create not supported for EmoFileSystem: " + f);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
            throws IOException {
        throw new IOException("Append not supported for EmoFileSystem: " + f);
    }

    @Override
    public boolean rename(Path src, Path dst)
            throws IOException {
        throw new IOException("Rename not supported for EmoFileSystem: " + src);
    }

    @Override
    public boolean delete(Path f, boolean recursive)
            throws IOException {
        throw new IOException("Delete not supported for EmoFileSystem: " + f);
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        throw new UnsupportedOperationException("Working directories not supported for EmoFileSystem");
    }

    @Override
    public Path getWorkingDirectory() {
        // Only one directory is supported, the base directory "/"
        return new Path("/");
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission)
            throws IOException {
        throw new IOException("Making directories not supported for EmoFileSystem: " + f);
    }
}
