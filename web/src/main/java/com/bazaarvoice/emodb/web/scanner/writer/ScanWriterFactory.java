package com.bazaarvoice.emodb.web.scanner.writer;

import com.google.common.base.Optional;

import java.net.URI;

/**
 * Interface used to create the available {@link ScanWriter} implementations.
 */
public interface ScanWriterFactory {

    FileScanWriter createFileScanWriter(int taskId, URI destination, Optional<Integer> maxOpenFiles);

    S3ScanWriter createS3ScanWriter(int taskId, URI destination, Optional<Integer> maxOpenFiles);

    DiscardingScanWriter createDiscardingScanWriter(int taskId, Optional<Integer> maxOpenFiles);

    KafkaScanWriter createKafkaScanWriter(URI destination);
}
