package com.bazaarvoice.emodb.web.scanner.writer;

import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import java.net.URI;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Generator class for creating a scan writer for a set of destinations.
 */
public class ScanWriterGenerator {

    private final ScanWriterFactory _scanWriterFactory;

    @Inject
    public ScanWriterGenerator(ScanWriterFactory scanWriterFactory) {
        _scanWriterFactory = scanWriterFactory;
    }

    /**
     * Creates a scan writer from the given desintations.
     */
    public ScanWriter createScanWriter(final int taskId, Set<ScanDestination> destinations) {
        checkArgument(!destinations.isEmpty(), "destinations.isEmpty()");

        if (destinations.size() == 1) {
            return createScanWriter(taskId, Iterables.getOnlyElement(destinations));
        }
        return new MultiScanWriter(ImmutableList.copyOf(
                Iterables.transform(destinations, new Function<ScanDestination, ScanWriter>() {
                    @Override
                    public ScanWriter apply(ScanDestination destination) {
                        return createScanWriter(taskId, destination);
                    }
                })
        ));
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
