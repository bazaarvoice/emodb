package com.bazaarvoice.emodb.web.scanner.writer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * ScanWriter that writes to a temporary file first, then transfers it.
 */
abstract public class TemporaryFileScanWriter extends AbstractScanWriter {
    private final Logger _log = LoggerFactory.getLogger(TemporaryFileScanWriter.class);

    private final Counter _openTransfers;
    private final Counter _blockedNewShards;

    // Default number of shards that can be written and/or transferred concurrently
    private final static int DEFAULT_MAX_OPEN_SHARDS = 20;

    private final Map<TransferKey, ShardFiles> _openShardFiles = Maps.newHashMap();
    private final int _maxOpenShards;
    private final ReentrantLock _lock = new ReentrantLock();
    private final Condition _shardFilesClosedOrExceptionCaught = _lock.newCondition();
    private volatile IOException _uploadException = null;
    private final ObjectMapper _mapper;

    protected TemporaryFileScanWriter(String type, int taskId, URI baseUri, Compression compression,
                                      MetricRegistry metricRegistry, Optional<Integer> maxOpenShards,
                                      ObjectMapper objectMapper) {
        super(type, taskId, baseUri, compression, metricRegistry);
        requireNonNull(maxOpenShards, "maxOpenShards");

        _maxOpenShards = maxOpenShards.or(DEFAULT_MAX_OPEN_SHARDS);
        checkArgument(_maxOpenShards > 0, "maxOpenShards <= 0");

        _openTransfers = metricRegistry.counter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "open-transfers"));
        _blockedNewShards = metricRegistry.counter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "blocked-new-shards"));
        _mapper = requireNonNull(objectMapper, "objectMapper");
    }

    /**
     * Implementation-specific call to asynchronously transfer the temporary file to the given URI.
     */
    abstract protected ListenableFuture<?> transfer(TransferKey transferKey, URI uri, File file);

    /**
     * Implementation-specific call to get TransferStatus objects for all active transfers.
     */
    abstract protected Map<TransferKey, TransferStatus> getStatusForActiveTransfers();

    @Override
    public ScanDestinationWriter writeShardRows(String tableName, final String placement, int shardId, long tableUuid)
            throws IOException, InterruptedException {
        final ShardFiles shardFiles = getShardFiles(shardId, tableUuid);

        // Fail immediately if there has already been a failure
        propagateExceptionIfPresent();
        // Don't allow another shard to start if there are too many outstanding transfers
        blockNewShardIfNecessary(shardFiles);

        if (shardFiles.isCanceled()) {
            throw new IOException("Shard file closed: " + shardFiles.getKey());
        }

        if (_closed) {
            throw new IOException("Writer closed");
        }

        final File shardFile = shardFiles.addShardFile();

        try {
            final URI uri = getUriForShard(tableName, shardId, tableUuid);
            OutputStream out = open(shardFile, getCounterForPlacement(placement));

            return new ShardWriter(out, _mapper) {
                @Override
                synchronized protected void ready(boolean isEmpty, Optional<Integer> finalPartCount)
                        throws IOException {
                    if (shardFiles.isCanceled()) {
                        throw new IOException("Shard file closed: " + shardFiles.getKey());
                    }
                    if (finalPartCount.isPresent()) {
                        shardFiles.setFinalPartCount(finalPartCount);
                    }
                    Optional<File> completeFile = shardFiles.shardFileComplete(isEmpty, shardFile);
                    if (completeFile == null) {
                        // File is not complete
                        return;
                    }

                    if (completeFile.isPresent()) {
                        // File is complete and has content
                        asyncTransfer(completeFile.get());
                    } else {
                        // File is complete but was empty so there is nothing to transfer.
                        closeShardFiles(shardFiles);
                    }
                }

                public void asyncTransfer(final File completeFile) {
                    _log.debug("Initiating async transfer: id={}, file={}, uri={}", _taskId, completeFile, uri);
                    _openTransfers.inc();
                    ListenableFuture<?> future = transfer(shardFiles.getKey(), uri, completeFile);
                    Futures.addCallback(future, new FutureCallback<Object>() {
                        @Override
                        public void onSuccess(Object result) {
                            transferComplete(shardFiles);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            if (!_closed) {
                                // Frequently a spurious exception related to cancellation is thrown if this instance
                                // has been closed.  Only register the exception if the instance has not been closed.
                                registerException(placement, t);
                            }
                            transferComplete(shardFiles);
                        }
                    });
                }

                @Override
                synchronized protected void cancel() {
                    shardFiles.setCanceled(true);
                    closeShardFiles(shardFiles);
                }

                public String toString() {
                    return uri.toString();
                }
            };
        } catch (IOException e) {
            shardFiles.deleteShardFile(shardFile);
            throw e;
        }
    }

    private ShardFiles getShardFiles(int shardId, long tableUuid) {
        TransferKey key = new TransferKey(tableUuid, shardId);
        ShardFiles shardFiles;

        _lock.lock();
        try {
            shardFiles = _openShardFiles.get(key);
            if (shardFiles == null) {
                shardFiles = new ShardFiles(key);
                _openShardFiles.put(key, shardFiles);
            }
        } finally {
            _lock.unlock();
        }

        return shardFiles;
    }

    /** Cleans up any files associated with the ShardFiles instance and removes it from then open set of ShardFiles. */
    private void closeShardFiles(ShardFiles shardFiles) {
        shardFiles.deleteAllShardFiles();
        _lock.lock();
        try {
            _openShardFiles.remove(shardFiles.getKey());
            _shardFilesClosedOrExceptionCaught.signalAll();
        } finally {
            _lock.unlock();
        }
    }

    /** Called when a shard file has been transferred, successfully or otherwise.  This also closes the ShardFiles. */
    private void transferComplete(ShardFiles shardFiles) {
        _log.debug("Transfer complete: id={}, file={}", _taskId, shardFiles.getFirstFile());
        closeShardFiles(shardFiles);
        _openTransfers.dec();
    }

    /**
     * If a previous upload has already failed then don't allow any more shards to be written.
     */
    private void propagateExceptionIfPresent() throws IOException {
        if (_uploadException != null) {
            throw _uploadException;
        }
    }

    private void blockNewShardIfNecessary(ShardFiles shardFiles)
            throws IOException, InterruptedException {
        if (maxOpenShardsGreaterThan(_maxOpenShards - 1, shardFiles.getKey())) {
            _blockedNewShards.inc();
            try {
                blockUntilOpenShardsAtMost(_maxOpenShards - 1, shardFiles.getKey());
            } finally {
                _blockedNewShards.dec();
            }
        }
    }

    /** Blocks until the number of open shards is equal to or less than the provided threshold. */
    private void blockUntilOpenShardsAtMost(int maxOpenShards, @Nullable TransferKey permittedKey)
            throws IOException, InterruptedException {
        blockUntilOpenShardsAtMost(maxOpenShards, permittedKey, Instant.MAX);
    }

    /**
     * Blocks until the number of open shards is equal to or less than the provided threshold or the current time
     * is after the timeout timestamp.
     */
    private boolean blockUntilOpenShardsAtMost(int maxOpenShards, @Nullable TransferKey permittedKey, Instant timeout)
            throws IOException, InterruptedException {
        Stopwatch stopWatch = Stopwatch.createStarted();
        Instant now;

        _lock.lock();
        try {
            while (!_closed &&
                    maxOpenShardsGreaterThan(maxOpenShards, permittedKey) &&
                    (now = Instant.now()).isBefore(timeout)) {
                // Stop blocking if there is an exception
                propagateExceptionIfPresent();

                // Wait no longer than 30 seconds; we want to log at least every 30 seconds we've been waiting.
                long waitTime = Math.min(Duration.ofSeconds(30).toMillis(), Duration.between(now, timeout).toMillis());
                _shardFilesClosedOrExceptionCaught.await(waitTime, TimeUnit.MILLISECONDS);

                if (!maxOpenShardsGreaterThan(maxOpenShards, permittedKey)) {
                    propagateExceptionIfPresent();
                    return true;
                }

                _log.debug("After {} seconds task {} still has {} open shards",
                        stopWatch.elapsed(TimeUnit.SECONDS), _taskId, _openShardFiles.size());
            }

            propagateExceptionIfPresent();
            return !maxOpenShardsGreaterThan(maxOpenShards, permittedKey);
        } finally {
            _lock.unlock();
        }
    }

    private boolean maxOpenShardsGreaterThan(int maxOpenShards, @Nullable TransferKey permittedKey) {
        return _openShardFiles.size() > maxOpenShards && // Too many open shards
                (permittedKey == null || !_openShardFiles.containsKey(permittedKey)); // Never block if the permitted key's shard is already open
    }

    private void registerException(String placement, Throwable e) {
        _log.error("Async transfer failed for task id={}, placement {}", _taskId, placement, e);
        IOException ioException;
        if (e instanceof IOException) {
            ioException = (IOException) e;
        } else {
            //noinspection ThrowableInstanceNeverThrown
            ioException = new IOException(e);
        }

        _lock.lock();
        try {
            if (_uploadException == null) {
                _uploadException = ioException;
                _shardFilesClosedOrExceptionCaught.signalAll();
            }
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public WaitForAllTransfersCompleteResult waitForAllTransfersComplete(Duration duration) throws IOException, InterruptedException {
        boolean complete = blockUntilOpenShardsAtMost(0, null, Instant.now().plus(duration));
        if (complete) {
            return new WaitForAllTransfersCompleteResult(ImmutableMap.<TransferKey, TransferStatus>of());
        }

        return new WaitForAllTransfersCompleteResult(getStatusForActiveTransfers());
    }

    @Override
    public void close() {
        super.close();

        _lock.lock();
        try {
            for (ShardFiles shardFiles : _openShardFiles.values()) {
                shardFiles.setCanceled(true);
                shardFiles.deleteAllShardFiles();
            }

            _openShardFiles.clear();
            _shardFilesClosedOrExceptionCaught.signalAll();
        } finally {
            _lock.unlock();
        }
    }

    private class ShardFiles {
        private final TransferKey _key;
        private final List<ShardFile> _parts = Lists.newArrayList();
        private Optional<Integer> _finalPartCount = Optional.absent();
        private volatile boolean _canceled;

        private ShardFiles(TransferKey key) {
            _key = key;
        }

        private TransferKey getKey() {
            return _key;
        }

        synchronized public void setFinalPartCount(Optional<Integer> finalPartCount) {
            if (finalPartCount.isPresent()) {
                if (!_finalPartCount.isPresent()) {
                    _finalPartCount = finalPartCount;
                } else if (!_finalPartCount.get().equals(finalPartCount.get())) {
                    throw new IllegalStateException("Shard set with inconsistent final part counts: " + _key);
                }
            }
        }

        public File addShardFile()
                throws IOException {
            File file = createTemporaryShardFile();
            ShardFile shardFile = new ShardFile(file);
            synchronized (this) {
                _parts.add(shardFile);
            }
            return file;
        }

        private File createTemporaryShardFile()
                throws IOException {
            return File.createTempFile(
                    format("emoshard_%02x_%016x", _key.getShardId(), _key.getTableUuid()), _compression.getExtension());
        }

        /**
         * Called when an individual shard file has been fully written.
         * @param isEmpty True if no data was written to the file.  This can happen if a shard contained only deleted content
         * @param file The file to transfer, assuming it is not empty
         * @return null if the shard contains at least one incomplete file, absent if the all files are complete but
         *         were all empty, or the File to transfer if the entire shard file is complete and not empty.
         */
        @Nullable
        synchronized public Optional<File> shardFileComplete(boolean isEmpty, File file)
                throws IOException {
            boolean allPartsAvailable = _finalPartCount.or(-1) == _parts.size();
            boolean allComplete = true;

            for (ShardFile shardFile : _parts) {
                if (file.equals(shardFile.getFile())) {
                    shardFile.complete(isEmpty);
                }
                allComplete = allComplete && shardFile.isComplete();
            }

            if (allPartsAvailable && allComplete) {
                // All parts have been written.  Reduce to the list of files which actually have content.
                List<File> files = FluentIterable.from(_parts)
                        .filter(_shardFileNotEmpty)
                        .transform(_getShardFileFile)
                        .toList();

                switch (files.size()) {
                    case 0:
                        // All files were empty
                        return Optional.absent();
                    case 1:
                        // There is only a single file; return it
                        return Optional.of(files.get(0));
                    default:
                        // Combine the files into a single file
                        File combinedFile = concatenateFiles(files, createTemporaryShardFile());
                        // Delete the uncombined files and set the combined file as the only file
                        deleteAllShardFiles();
                        _parts.clear();
                        _parts.add(new ShardFile(combinedFile, true, false));
                        return Optional.of(combinedFile);
                }
            } else {
                // File not ready to transfer
                return null;
            }
        }

        private void deleteAllShardFiles() {
            for (ShardFile shardFile : _parts) {
                File file = shardFile.getFile();
                deleteShardFile(file);
            }
        }

        public void deleteShardFile(File file) {
            if (!file.delete()) {
                _log.warn("Failed to delete file: {}", file);
            } else {
                _log.debug("Deleted temporary file : {}", file);
            }
        }

        public File getFirstFile() {
            return _parts.get(0).getFile();
        }

        public  boolean isCanceled() {
            return _canceled;
        }

        public void setCanceled(boolean canceled) {
            _canceled = canceled;
        }
    }

    private static class ShardFile {
        private final File _file;
        private boolean _complete;
        private boolean _isEmpty;

        private ShardFile(File file) {
            this(file, false, false);
        }

        private ShardFile(File file, boolean complete, boolean isEmpty) {
            _file = file;
            _complete = complete;
            _isEmpty = isEmpty;
        }

        private File getFile() {
            return _file;
        }

        private boolean isComplete() {
            return _complete;
        }

        private void complete(boolean isEmpty) {
            _complete = true;
            _isEmpty = isEmpty;
        }

        public boolean isEmpty() {
            return _isEmpty;
        }
    }

    /** Simple predicate to filter out ShardFiles that are empty. */
    private static Predicate<ShardFile> _shardFileNotEmpty = new Predicate<ShardFile>() {
        @Override
        public boolean apply(ShardFile shardFile) {
            return !shardFile.isEmpty();
        }
    };

    /** Simple function to return the local temporary File associated with a ShardFile. */
    private static Function<ShardFile, File> _getShardFileFile = new Function<ShardFile, File>() {
        @Override
        public File apply(ShardFile shardFile) {
            return shardFile.getFile();
        }
    };
}

