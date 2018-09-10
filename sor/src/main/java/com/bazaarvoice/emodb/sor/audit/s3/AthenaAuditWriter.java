package com.bazaarvoice.emodb.sor.audit.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.audit.AuditFlusher;
import com.bazaarvoice.emodb.sor.audit.AuditWriter;
import com.bazaarvoice.emodb.sor.audit.AuditWriterConfiguration;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.dropwizard.util.Size;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Audit writer implementation which writes all audits as GZIP'd jsonl documents to S3 partitioned by date.
 * This format allows audit queries to be carried out by Athena, Amazon's Presto implementation over S3 documents.
 *
 * This audit writer favors fast, non-blocking calls to {@link #persist(String, String, Audit, long)} over guaranteeing
 * a completely loss-less audit history.  To achieve this all audits are written to an in-memory queue.  That queue
 * is written to a local log file until it has reached a maximum size or age, both configurable in the constructor.
 * At this time the file is asynchronously GZIP'd then delivered to S3.  Once the file is delivered it is deleted from
 * the local host.
 *
 * Each stage has multiple layers of recovery, ensuring that once a line is written to a file that file will eventually
 * be delivered to S3.  The exceptions to this which can cause audit loss are:
 *
 * <ol>
 *     <li>The process is terminated while unwritten audits are still in the audit queue.</li>
 *     <li>The host itself terminates before all files are delivered to S3.</li>
 * </ol>
 */
public class AthenaAuditWriter implements AuditWriter, AuditFlusher {

    private final static Logger _log = LoggerFactory.getLogger(AthenaAuditWriter.class);

    private final static DateTimeFormatter LOG_FILE_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC);

    private final static String OPEN_FILE_SUFFIX = ".log.tmp";
    private final static String CLOSED_FILE_SUFFIX = ".log";
    private final static String COMPRESSED_FILE_SUFFIX = ".log.gz";

    private final static long DEFAULT_MAX_FILE_SIZE = Size.megabytes(10).toBytes();
    private final static Duration DEFAULT_MAX_BATCH_TIME = Duration.ofMinutes(2);

    private final AmazonS3 _s3;
    private final String _s3Bucket;
    private final String _s3AuditRoot;
    private final long _maxFileSize;
    private final long _maxBatchTimeMs;
    private final File _stagingDir;
    private final String _logFilePrefix;
    private final BlockingQueue<QueuedAudit> _auditQueue;
    private final Clock _clock;
    private final ObjectWriter _objectWriter;
    private final ConcurrentMap<Long, AuditOutput> _openAuditOutputs = Maps.newConcurrentMap();

    private ScheduledExecutorService _auditService;
    private ExecutorService _fileTransferService;
    private AuditOutput _mruAuditOutput;
    private boolean _fileTransfersEnabled;

    @Inject
    public AthenaAuditWriter(AuditWriterConfiguration config, AmazonS3 s3, ObjectMapper objectMapper, Clock clock) {

        this(s3, config.getLogBucket(), config.getLogPath(), config.getMaxFileSize(),
                Duration.ofMillis(config.getMaxBatchTime().getMillis()),
                config.getStagingDir() != null ? new File(config.getStagingDir()) : com.google.common.io.Files.createTempDir(),
                config.getLogFilePrefix(), objectMapper, clock, config.isFileTransfersEnabled(), null, null);
    }

    @VisibleForTesting
    AthenaAuditWriter(AmazonS3 s3, String s3Bucket, String s3Path, long maxFileSize, Duration maxBatchTime,
                      File stagingDir, String logFilePrefix, ObjectMapper objectMapper, Clock clock,
                      boolean fileTransfersEnabled, ScheduledExecutorService auditService,
                      ExecutorService fileTransferService) {

        _s3 = requireNonNull(s3);
        _s3Bucket = requireNonNull(s3Bucket);

        String s3AuditRoot = s3Path;
        if (s3AuditRoot.startsWith("/")) {
            s3AuditRoot = s3AuditRoot.substring(1);
        }
        if (s3AuditRoot.endsWith("/")) {
            s3AuditRoot = s3AuditRoot.substring(0, s3AuditRoot.length()-1);
        }
        _s3AuditRoot = s3AuditRoot;

        checkArgument(stagingDir.exists(), "Staging directory must exist");

        _maxFileSize = maxFileSize > 0 ? maxFileSize : DEFAULT_MAX_FILE_SIZE;
        _maxBatchTimeMs = (maxBatchTime != null && maxBatchTime.compareTo(Duration.ZERO) > 0 ? maxBatchTime : DEFAULT_MAX_BATCH_TIME).toMillis();
        _stagingDir = requireNonNull(stagingDir, "stagingDir");
        _logFilePrefix = requireNonNull(logFilePrefix, "logFilePrefix");
        _clock = requireNonNull(clock, "clock");

        // Audit queue isn't completely unbounded but is large enough to ensure at several times the normal write rate
        // it can accept audits without blocking.
        _auditQueue = new ArrayBlockingQueue<>(4096);

        // Need to ensure the object mapper keeps the file stream open after each audit is written.
        _objectWriter = objectMapper.copy()
                .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
                .writer();

        _fileTransfersEnabled = fileTransfersEnabled;

        // Two threads for the audit service: once to drain queued audits and one to close audit logs files and submit
        // them for transfer.  Normally these are initially null and locally managed, but unit tests may provide
        // pre-configured instances.
        _auditService = Optional.ofNullable(auditService)
                .orElse(Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder().setNameFormat("audit-log-%d").build()));
        _fileTransferService = Optional.ofNullable(fileTransferService)
                .orElse(Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("audit-transfer-%d").build()));


        long now = _clock.millis();
        long msToNextBatch = _maxBatchTimeMs - (now % _maxBatchTimeMs);

        // Do a one-time closing of all orphaned log files which may have been left behind if a previous EmoDB process
        // terminated before closing them.
        for (final File logFile : _stagingDir.listFiles((dir, name) -> name.startsWith(_logFilePrefix) && name.endsWith(OPEN_FILE_SUFFIX))) {
            if (logFile.length() > 0) {
                try {
                    renameClosedLogFile(logFile);
                } catch (IOException e) {
                    _log.warn("Failed to close orphaned audit log file: {}", logFile, e);
                }
            } else {
                if (!logFile.delete()) {
                    _log.debug("Failed to delete empty orphaned log file: {}", logFile);
                }
            }
        }

        _auditService.scheduleWithFixedDelay(() -> processQueuedAudits(true),
                0, 1, TimeUnit.SECONDS);

        _auditService.scheduleAtFixedRate(this::doLogFileMaintenance,
                msToNextBatch, _maxBatchTimeMs, TimeUnit.MILLISECONDS);
        }

    @Override
    public void flushAndShutdown() {
        _auditService.shutdown();
        try {
            if (!_auditService.awaitTermination(5, TimeUnit.SECONDS)) {
                _log.warn("Audits service did not shutdown cleanly.");
                _auditService.shutdownNow();
            } else {
                // Poll the queue one last time and drain anything that is still remaining
                processQueuedAudits(false);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Close all log files and prepare them for transfer.
        closeCompleteLogFiles(true);
        prepareClosedLogFilesForTransfer();
        transferLogFilesToS3();

        _fileTransferService.shutdown();

        try {
            if (_fileTransferService.awaitTermination(30, TimeUnit.SECONDS)) {
                _log.info("All audits were successfully persisted prior to shutdown");
            } else {
                _log.warn("All audits could not be persisted prior to shutdown");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void persist(String table, String key, Audit audit, long auditTime) {
        try {
            _auditQueue.put(new QueuedAudit(table, key, audit, auditTime));
        } catch (InterruptedException e) {
            // Don't error out if the audit was interrupted since this implementation does not guarantee 100%
            // audit retention, just warn that it happened.
            _log.warn("Interrupted attempting to write audit for {}/{}", table, key);
        }
    }

    /**
     * This method is run at regular intervals to close log files, gzip them and initiate their transfer to S3.
     */
    private void doLogFileMaintenance() {
        // Close all files that have either exceeded their maximum size or age but have not closed due to lack of
        // audit activity.
        closeCompleteLogFiles(false);
        prepareClosedLogFilesForTransfer();
        transferLogFilesToS3();
    }

    private void closeCompleteLogFiles(boolean forceClose) {
        for (AuditOutput auditOutput : ImmutableList.copyOf(_openAuditOutputs.values())) {
            if (forceClose || auditOutput.shouldClose()) {
                auditOutput.close();
            }
        }
    }

    /**
     * This method takes all closed log files and GZIPs and renames them in preparation for transfer.  If the operation
     * fails the original file is unmodified so the next call should attempt to prepare the file again.  This means
     * the same file may be transferred more than once, but this guarantees that so long as the host remains active the
     * file will eventually be transferred.
     */
    private void prepareClosedLogFilesForTransfer() {
        for (final File logFile : _stagingDir.listFiles((dir, name) -> name.startsWith(_logFilePrefix) && name.endsWith(CLOSED_FILE_SUFFIX))) {
            boolean moved;
            String fileName = logFile.getName().substring(0, logFile.getName().length() - CLOSED_FILE_SUFFIX.length()) + COMPRESSED_FILE_SUFFIX;
            try (FileInputStream fileIn = new FileInputStream(logFile);
                 FileOutputStream fileOut = new FileOutputStream(new File(logFile.getParentFile(), fileName));
                 GzipCompressorOutputStream gzipOut = new GzipCompressorOutputStream(fileOut)) {

                ByteStreams.copy(fileIn, gzipOut);
                moved = true;
            } catch (IOException e) {
                _log.warn("Failed to compress audit log file: {}", logFile, e);
                moved = false;
            }

            if (moved) {
                if (!logFile.delete()) {
                    _log.warn("Failed to delete audit log file: {}", logFile);
                }
            }
        }
    }

    /**
     * This method takes all log files prepared by {@link #prepareClosedLogFilesForTransfer()} and initiates their
     * transfer to S3.  The transfer itself is performed asynchronously.
     */
    private void transferLogFilesToS3() {
        if (_fileTransfersEnabled) {
            // Find all closed log files in the staging directory and move them to S3
            for (final File logFile : _stagingDir.listFiles((dir, name) -> name.startsWith(_logFilePrefix) && name.endsWith(COMPRESSED_FILE_SUFFIX))) {
                // Extract the date portion of the file name and is it to partition the file in S3
                String auditDate = logFile.getName().substring(_logFilePrefix.length() + 1, _logFilePrefix.length() + 9);
                String dest = String.format("%s/date=%s/%s", _s3AuditRoot, auditDate, logFile.getName());

                _fileTransferService.submit(() -> {
                    // Since file transfers are done in a single thread, there shouldn't be any concurrency issues,
                    // but verify the same file wasn't submitted previously and is already transferred.
                    if (logFile.exists()) {
                        try {
                            PutObjectResult result = _s3.putObject(_s3Bucket, dest, logFile);
                            _log.debug("Audit log copied: {}, ETag={}", logFile, result.getETag());

                            if (!logFile.delete()) {
                                _log.warn("Failed to delete file after copying to s3: {}", logFile);
                            }
                        } catch (Exception e) {

                            // Log the error, try again on the next iteration
                            _log.warn("Failed to copy log file {}", logFile, e);
                        }
                    }
                });
            }
        }

    }

    /**
     * This method is run at regular intervals to remove audits from the audit queue and write them to a local file.
     */
    private void processQueuedAudits(boolean interruptable) {
        QueuedAudit audit;
        try {
            while (!(_auditService.isShutdown() && interruptable) && ((audit = _auditQueue.poll()) != null)) {
                boolean written = false;
                while (!written) {
                    AuditOutput auditOutput = getAuditOutputForTime(audit.time);
                    written = auditOutput.writeAudit(audit);
                }
            }
        } catch (Exception e) {
            _log.error("Processing of queued audits failed", e);
        }
    }

    private AuditOutput getAuditOutputForTime(long time) {
        // Truncate the time based on the batch duration
        long batchTime = time - (time % _maxBatchTimeMs);
        // The most common case is that audits are written in time order, so optimize by caching the most recently
        // used AuditOutput and return it if it is usable for an audit at the given time.
        AuditOutput mruAuditOutput = _mruAuditOutput;
        if (mruAuditOutput != null && batchTime == mruAuditOutput.getBatchTime() && !mruAuditOutput.isClosed()) {
            return mruAuditOutput;
        }

        return _mruAuditOutput = _openAuditOutputs.computeIfAbsent(batchTime, this::createNewAuditLogOut);
    }

    private AuditOutput createNewAuditLogOut(long batchTime) {
        // Set the batch to close at the end of the next batch time from now, regardless of what time the batch is for.
        long now = _clock.millis();
        long nextBatchCycleCloseTime = now - (now % _maxBatchTimeMs) + _maxBatchTimeMs;

        return new AuditOutput(LOG_FILE_DATE_FORMATTER.format(Instant.ofEpochMilli(batchTime)), batchTime, nextBatchCycleCloseTime);
    }

    private void renameClosedLogFile(File logFile) throws IOException {
        // Move the file to a new file without the ".tmp" suffix
        String closedFileName = logFile.getName().substring(0, logFile.getName().length() - OPEN_FILE_SUFFIX.length()) + CLOSED_FILE_SUFFIX;
        Files.move(logFile.toPath(), new File(logFile.getParentFile(), closedFileName).toPath());
    }

    /**
     * In-memory holder for an audit in the queue.
     */
    private static class QueuedAudit {
        final String table;
        final String key;
        final Audit audit;
        final long time;

        QueuedAudit(String table, String key, Audit audit, long time) {
            this.table = table;
            this.key = key;
            this.audit = audit;
            this.time = time;
        }
    }

    /**
     * Class holder for a single audit output file, with most of the file details abstracted by {@link #writeAudit(QueuedAudit)}.
     */
    private class AuditOutput {
        private final File _auditLogFile;
        private final long _batchTime;
        private final long _closeTime;
        private final ReentrantLock _lock = new ReentrantLock();
        private volatile boolean _closed;
        private volatile int _auditsWritten = 0;
        private volatile CountingOutputStream _auditLogOut;

        AuditOutput(String datePrefix, long batchTime, long closeTime) {
            String fileName = String.format("%s.%s.%s%s", _logFilePrefix, datePrefix, UUID.randomUUID(), OPEN_FILE_SUFFIX);
            _auditLogFile = new File(_stagingDir, fileName);
            _batchTime = batchTime;
            _closeTime = closeTime;
        }

        void createAuditLogOut() throws IOException {
            FileOutputStream fileOut = new FileOutputStream(_auditLogFile);
            _auditLogOut = new CountingOutputStream(fileOut);
        }

        /**
         * Writes a single audit to the log file.
         * @return True if the audit was written, false if the audit could not be written because the file was closed
         *         or can no longer accept writes due to file size or age.
         */
        boolean writeAudit(QueuedAudit audit) {
            Map<String, Object> auditMap = Maps.newLinkedHashMap();
            // This is an intentional break from convention to use "tablename" instead of "table".  This is because
            // "table" is a reserved word in Presto and complicates queries for that column.
            auditMap.put("tablename", audit.table);
            auditMap.put("key", audit.key);
            auditMap.put("time", audit.time);
            // Even though the content of the audit is valid JSON the potential key set is unbounded.  This makes
            // it difficult to define a schema for Presto.  So create values for the conventional keys and store
            // the rest in an opaque blob.
            Map<String, Object> custom = new HashMap<>(audit.audit.getAll());
            if (custom.remove(Audit.COMMENT) != null) {
                auditMap.put("comment", audit.audit.getComment());
            }
            if (custom.remove(Audit.HOST) != null) {
                auditMap.put("host", audit.audit.getHost());
            }
            if (custom.remove(Audit.PROGRAM) != null) {
                auditMap.put("program", audit.audit.getProgram());
            }
            if (custom.remove(Audit.SHA1) != null) {
                auditMap.put("sha1", audit.audit.getCustom(Audit.SHA1));
            }
            if (custom.remove(Audit.TAGS) != null) {
                auditMap.put("tags", audit.audit.getTags());
            }
            if (!custom.isEmpty()) {
                try {
                    auditMap.put("custom", _objectWriter.writeValueAsString(custom));
                } catch (JsonProcessingException e) {
                    _log.info("Failed to write custom audit information", e);
                }
            }

            // Lock critical section to ensure the file isn't closed while writing the audit
            _lock.lock();
            try {
                if (shouldClose()) {
                    close();
                }

                if (isClosed()) {
                    return false;
                }

                // Lazily create the audit log file on the first write.  This way if a race occurs and more than
                // one AuditOutput is created for a batch only the one which wins will actually generate a file to
                // be transferred to S3.
                if (_auditLogOut == null) {
                    createAuditLogOut();
                }

                _objectWriter.writeValue(_auditLogOut, auditMap);
                _auditLogOut.write('\n');
                //noinspection NonAtomicOperationOnVolatileField
                _auditsWritten += 1;
            } catch (IOException e) {
                _log.warn("Failed to write audit to logs", e);
            } finally {
                _lock.unlock();
            }

            return true;
        }

        boolean isClosed() {
            return _closed;
        }

        void close() {
            _lock.lock();
            try {
                if (!_closed) {
                    _closed = true;

                    if (_auditLogOut != null) {
                        _auditLogOut.close();
                    }

                    if (_auditsWritten != 0) {
                        renameClosedLogFile(_auditLogFile);
                    }
                }
            } catch (IOException e) {
                _log.warn("Failed to close log file", e);
            } finally {
                _openAuditOutputs.remove(_batchTime, this);
                _lock.unlock();
            }
        }

        long getBatchTime() {
            return _batchTime;
        }

        boolean isBatchTimedOut() {
            return _clock.millis() >= _closeTime;
        }

        boolean isOversized() {
            return _auditLogOut != null && _auditLogOut.getCount() > _maxFileSize;
        }

        boolean shouldClose() {
            return isBatchTimedOut() || isOversized();
        }
    }
}
