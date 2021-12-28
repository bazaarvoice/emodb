package com.bazaarvoice.emodb.web.scanner.writer;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * ScanWriter implementation which allows writing to multiple ScanWriters concurrently.
 */
public class MultiScanWriter implements ScanWriter {

    private final List<ScanWriter> _scanWriters;

    public MultiScanWriter(List<ScanWriter> scanWriters) {
        requireNonNull(scanWriters, "scanWriters");
        checkArgument(!scanWriters.isEmpty(), "scanWriters.isEmpty()");
        _scanWriters = scanWriters;
    }

    @Override
    public ScanDestinationWriter writeShardRows(String tableName, String placement, int shardId, long tableUuid)
            throws IOException, InterruptedException {

        final List<ScanDestinationWriter> scanDestinationWriters = Lists.newArrayListWithCapacity(_scanWriters.size());

        // Create an output stream that tees to all of the underling shard writer's output streams
        for (ScanWriter scanWriter : _scanWriters) {
            ScanDestinationWriter scanDestinationWriter = scanWriter.writeShardRows(tableName, placement, shardId, tableUuid);
            scanDestinationWriters.add(scanDestinationWriter);
        }

        return new ScanDestinationWriter() {
            @Override
            public void writeDocument(Map<String, Object> document) throws IOException, InterruptedException {
                for (ScanDestinationWriter scanDestinationWriter : scanDestinationWriters) {
                    scanDestinationWriter.writeDocument(document);
                }
            }

            @Override
            public void closeAndCancel() {
                for (ScanDestinationWriter scanDestinationWriter : scanDestinationWriters) {
                    scanDestinationWriter.closeAndCancel();
                }
            }

            @Override
            public void closeAndTransferAsync(Optional<Integer> finalPartCount) throws IOException {
                for (ScanDestinationWriter scanDestinationWriter : scanDestinationWriters) {
                    scanDestinationWriter.closeAndTransferAsync(finalPartCount);
                }
            }
        };
    }

    @Override
    public WaitForAllTransfersCompleteResult waitForAllTransfersComplete(Duration duration) throws IOException, InterruptedException {
        Map<TransferKey, TransferStatus> allStatuses = Maps.newHashMap();
        for (ScanWriter scanWriter : _scanWriters) {
            WaitForAllTransfersCompleteResult result = scanWriter.waitForAllTransfersComplete(duration);
            if (!result.isComplete()) {
                allStatuses.putAll(result.getActiveTransferStatusMap());
            }
        }
        return new WaitForAllTransfersCompleteResult(allStatuses);
    }

    @Override
    public boolean writeScanComplete(String scanId, Date startTime)
            throws IOException {
        boolean allComplete = true;
        for (ScanWriter scanWriter : _scanWriters) {
            boolean writerComplete = scanWriter.writeScanComplete(scanId, startTime);
            allComplete = allComplete && writerComplete;
        }
        return allComplete;
    }

    @Override
    public void close() {
        Exception exception = null;

        for (ScanWriter scanWriter : _scanWriters) {
            try {
                scanWriter.close();
            } catch (Exception e) {
                // Keep track of the first exception, but attempt to close them all
                if (exception == null) {
                    exception = e;
                }
            }
        }

        if (exception != null) {
            Throwables.throwIfUnchecked(exception);
            throw new RuntimeException(exception);
        }
    }
}
