package com.bazaarvoice.emodb.web.scanner.writer;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Date;

public class UnsupportedKafkaScanWriter implements KafkaScanWriter {

    @Inject
    public UnsupportedKafkaScanWriter(@Assisted URI baseUri) {
        throw new IllegalArgumentException("Kafka destination is not supported by this instance");
    }

    @Override
    public ScanDestinationWriter writeShardRows(String tableName, String placement, int shardId, long tableUuid) throws IOException, InterruptedException {
        throw new IllegalArgumentException("Kafka destination is not supported by this instance");
    }

    @Override
    public WaitForAllTransfersCompleteResult waitForAllTransfersComplete(Duration duration) throws IOException, InterruptedException {
        throw new IllegalArgumentException("Kafka destination is not supported by this instance");
    }

    @Override
    public boolean writeScanComplete(String scanId, Date startTime) throws IOException {
        throw new IllegalArgumentException("Kafka destination is not supported by this instance");
    }

    @Override
    public void close() throws IOException {

    }
}
