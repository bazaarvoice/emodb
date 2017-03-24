package com.bazaarvoice.emodb.databus.tracer;

import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.databus.api.BlobCSVEventTracerSpec;
import com.bazaarvoice.emodb.databus.api.DatabusEventTracerSpec;
import com.bazaarvoice.emodb.event.api.EventTracer;
import com.google.inject.Inject;

/**
 * Factory for producing {@link EventTracer} instances from {@link DatabusEventTracerSpec} specifications.
 * Current implementation supports a single specification, {@link BlobCSVEventTracerSpec}.
 */
public class DatabusEventTracerFactory {

    private final BlobStore _blobStore;

    @Inject
    public DatabusEventTracerFactory(BlobStore blobStore) {
        _blobStore = blobStore;
    }

    public EventTracer createTracer(DatabusEventTracerSpec spec) {
        if (spec == null) {
            return null;
        }

        if (spec instanceof BlobCSVEventTracerSpec) {
            BlobCSVEventTracerSpec blobSpec = (BlobCSVEventTracerSpec) spec;
            return new BlobCSVEventTracer(_blobStore, blobSpec);
        }

        throw new IllegalArgumentException("Unsupported tracer specification: " + spec.getClass());
    }
}
