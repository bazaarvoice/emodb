package com.bazaarvoice.emodb.web.megabus;

import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.bazaarvoice.emodb.web.scanner.writer.ScanWriter;
import com.bazaarvoice.emodb.web.scanner.writer.ScanWriterGenerator;
import com.google.inject.Inject;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MegabusScanWriterGenerator extends ScanWriterGenerator {

    private final KafkaScanWriterFactory _kafkaScanWriterFactory;

    @Inject
    public MegabusScanWriterGenerator(KafkaScanWriterFactory kafkaScanWriterFactory) {
        _kafkaScanWriterFactory = requireNonNull(kafkaScanWriterFactory);
    }

    @Override
    public ScanWriter createScanWriter(int taskId, ScanDestination destination) {

        checkArgument(!destination.isDiscarding());
        checkArgument(destination.getUri().getScheme().equals("kafka"));

        return _kafkaScanWriterFactory.createKafkaScanWriter(destination.getUri());
    }
}
