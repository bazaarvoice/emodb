package com.bazaarvoice.emodb.web.scanner.writer;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;

/**
 * ScanWriter implementation which discards scan files but goes through the usual motions.  Useful for unit testing
 * and gathering metrics.
 */
public class DiscardingScanWriter extends TemporaryFileScanWriter {

    @Inject
    public DiscardingScanWriter(@Assisted int taskId, @Assisted Optional<Integer> maxOpenShards,
                                MetricRegistry metricRegistry, ObjectMapper objectMapper) {
        super("discarded", taskId, new File("/dev/null").toURI(), Compression.GZIP, metricRegistry, maxOpenShards, objectMapper);
    }

    @Override
    protected ListenableFuture<?> transfer(TransferKey transferKey, URI uri, File file) {
        return Futures.immediateFuture(null);
    }

    @Override
    protected Map<TransferKey, TransferStatus> getStatusForActiveTransfers() {
        return ImmutableMap.of();
    }

    @Override
    protected boolean writeScanCompleteFile(URI fileUri, byte[] contents)
            throws IOException {
        return true;
    }

    @Override
    protected void writeLatestFile(URI fileUri, byte[] contents)
            throws IOException {
        // empty
    }
}
