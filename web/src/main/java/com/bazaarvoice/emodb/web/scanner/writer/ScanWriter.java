package com.bazaarvoice.emodb.web.scanner.writer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Date;

/**
 * Base interface for writing {@link ScanWriter} files to a file system.
 */
public interface ScanWriter extends Closeable {
    /**
     * Creates a new {@link ShardWriter} for writing a particular table, shard and batch to the file system.
     * The contents are actually written by writing to {@link ShardWriter#getOutputStream()}
     * then transferred to the file system using {@link ShardWriter#closeAndTransferAysnc(com.google.common.base.Optional)}.
     */
    ShardWriter writeShardRows(String tableName, String placement, int shardId, long tableUuid)
            throws IOException, InterruptedException;

    /**
     * Synchronously blocks for up to the provided amount of time until all shards currently being transferred are
     * complete.  If any write fails this method will throw the source IOException.
     * @return true if all transfers were complete, false if it returned due to timeout.
     */
    WaitForAllTransfersCompleteResult waitForAllTransfersComplete(Duration duration)
            throws IOException, InterruptedException;

    /**
     * Writes a signal file that the scan is complete.
     * @return true if the file was written, false if it already existed (repeat call)
     */
    boolean writeScanComplete(String scanId, Date startTime)
            throws IOException;
}
