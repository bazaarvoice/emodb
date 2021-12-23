package com.bazaarvoice.emodb.web.scanner.writer;

import java.net.URI;
import java.util.Optional;

/**
 * Interface used to create the available {@link ScanWriter} implementations.
 */
public interface ScanWriterFactory {

    FileScanWriter createFileScanWriter(int taskId, URI destination, Optional<Integer> maxOpenFiles);

    S3ScanWriter createS3ScanWriter(int taskId, URI destination, Optional<Integer> maxOpenFiles);

    DiscardingScanWriter createDiscardingScanWriter(int taskId, Optional<Integer> maxOpenFiles);
}
