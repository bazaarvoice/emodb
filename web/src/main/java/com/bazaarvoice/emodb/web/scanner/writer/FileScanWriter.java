package com.bazaarvoice.emodb.web.scanner.writer;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * ScanWriter implementation for writing files to the local file system.
 */
public class FileScanWriter extends TemporaryFileScanWriter {

    private Map<TransferKey, File> _activeFiles = Maps.newConcurrentMap();

    @Inject
    public FileScanWriter(@Assisted int taskId, @Assisted URI baseUri, @Assisted Optional<Integer> maxOpenShards,
                          MetricRegistry metricRegistry, ObjectMapper objectMapper) {
        super("file", taskId, baseUri, Compression.GZIP, metricRegistry, maxOpenShards, objectMapper);
    }

    @Override
    protected ListenableFuture<?> transfer(TransferKey transferKey, URI uri, File file){
        _activeFiles.put(transferKey, file);
        try {
            File dest = new File(uri.toURL().getFile());
            Files.createParentDirs(dest);
            Files.copy(file, dest);
            return Futures.immediateFuture(null);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        } finally {
            _activeFiles.remove(transferKey);
        }

    }

    protected FileScanWriter(String type, int taskId, URI baseUri, Compression compression,
                             Optional<Integer> maxOpenShards, MetricRegistry metricRegistry, ObjectMapper objectMapper) {
        super(type, taskId, baseUri, compression, metricRegistry, maxOpenShards, objectMapper);
    }

    @Override
    protected Map<TransferKey, TransferStatus> getStatusForActiveTransfers() {
        Map<TransferKey, TransferStatus> statusMap = Maps.newHashMap();
        for (Map.Entry<TransferKey, File> entry : _activeFiles.entrySet()) {
            statusMap.put(entry.getKey(), new TransferStatus(entry.getKey(), entry.getValue().length(), 1, 0));
        }
        return statusMap;
    }

    @Override
    protected boolean writeScanCompleteFile(URI fileUri, byte[] contents)
            throws IOException {
        File file = new File(fileUri.toURL().getFile());
        if (file.exists()) {
            // Complete already marked; take no action
            return false;

        }
        Files.createParentDirs(file);
        Files.write(contents, file);
        return true;
    }

    @Override
    protected void writeLatestFile(URI fileUri, byte[] contents)
            throws IOException {
        File file = new File(fileUri.toURL().getFile());
        Files.createParentDirs(file);
        Files.write(contents, file);
    }
}
