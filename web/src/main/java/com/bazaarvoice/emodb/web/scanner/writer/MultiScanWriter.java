package com.bazaarvoice.emodb.web.scanner.writer;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.output.TeeOutputStream;
import org.joda.time.Duration;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * ScanWriter implementation which allows writing to multiple ScanWriters concurrently.
 */
public class MultiScanWriter implements ScanWriter {

    private final List<ScanWriter> _scanWriters;

    public MultiScanWriter(List<ScanWriter> scanWriters) {
        checkNotNull(scanWriters, "scanWriters");
        checkArgument(!scanWriters.isEmpty(), "scanWriters.isEmpty()");
        _scanWriters = scanWriters;
    }

    @Override
    public ShardWriter writeShardRows(ShardMetadata metadata)
            throws IOException, InterruptedException {

        final List<ShardWriter> shardWriters = Lists.newArrayListWithCapacity(_scanWriters.size());
        OutputStream out = null;

        // Create an output stream that tees to all of the underling shard writer's output streams
        for (ScanWriter scanWriter : _scanWriters) {
            ShardWriter shardWriter = scanWriter.writeShardRows(metadata);
            shardWriters.add(shardWriter);

            out = (out == null) ? shardWriter.getOutputStream()
                    : new TeeOutputStream(out, shardWriter.getOutputStream());
        }

        return new ShardWriter(out) {
            @Override
            protected void ready(boolean isEmpty, Optional<Integer> finalPartCount)
                    throws IOException {
                // Notify each shard writer that the file is ready
                for (ShardWriter shardWriter : shardWriters) {
                    shardWriter.ready(isEmpty, finalPartCount);
                }
            }
            @Override
            protected void cancel() {
                // Cancel each of the underlying writer's files
                for (ShardWriter shardWriter : shardWriters) {
                    shardWriter.cancel();
                }
            }
        };
    }

    @Override
    public WaitForAllTransfersCompleteResult waitForAllTransfersComplete(Duration duration) throws IOException, InterruptedException {
        Map<ShardMetadata, TransferStatus> allStatuses = Maps.newHashMap();
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
            throw Throwables.propagate(exception);
        }
    }
}
