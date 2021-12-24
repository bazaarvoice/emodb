package com.bazaarvoice.emodb.web.scanner.writer;

import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Generator class for creating a scan writer for a set of destinations.
 */
public abstract class ScanWriterGenerator {

    /**
     * Creates a scan writer from the given destinations.
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
    abstract public ScanWriter createScanWriter(int taskId, ScanDestination destination);

}
