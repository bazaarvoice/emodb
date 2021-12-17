package com.bazaarvoice.emodb.blob.api;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

public final class DefaultBlob extends DefaultBlobMetadata implements Blob, StreamSupplier {
    private final Range _range;
    private final StreamSupplier _streamSupplier;

    public DefaultBlob(BlobMetadata metadata, Range byteRange, StreamSupplier streamSupplier) {
        super(metadata);
        _range = requireNonNull(byteRange, "byteRange");
        _streamSupplier = requireNonNull(streamSupplier, "streamSupplier");
    }

    @Override
    public Range getByteRange() {
        return _range;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        _streamSupplier.writeTo(out);
    }
}
