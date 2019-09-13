package com.bazaarvoice.emodb.web.scanner.writer;

import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import java.net.URI;

public class DefaultScanWriterGenerator extends ScanWriterGenerator {
    private final ScanWriterFactory _scanWriterFactory;

    @Inject
    public DefaultScanWriterGenerator(ScanWriterFactory scanWriterFactory) {
        _scanWriterFactory = scanWriterFactory;
    }

    /**
     * Creates a scan writer for the given destination.
     */
    public ScanWriter createScanWriter(int taskId, ScanDestination destination) {
        if (destination.isDiscarding()) {
            return _scanWriterFactory.createDiscardingScanWriter(taskId, Optional.<Integer>absent());
        }

        URI uri = destination.getUri();
        String scheme = uri.getScheme();

        if ("file".equals(scheme)) {
            return _scanWriterFactory.createFileScanWriter(taskId, uri, Optional.<Integer>absent());
        }

        if ("s3".equals(scheme)) {
            return _scanWriterFactory.createS3ScanWriter(taskId, uri, Optional.<Integer>absent());
        }

        throw new IllegalArgumentException("Unsupported destination: " + destination);
    }
}
