package com.bazaarvoice.emodb.blob.api;

import java.io.IOException;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkNotNull;

public final class DefaultBlob extends DefaultBlobMetadata implements Blob, StreamSupplier {
    private final Range _range;
    private final StreamSupplier _streamSupplier;

    public DefaultBlob(BlobMetadata metadata, Range byteRange, StreamSupplier streamSupplier) {
        super(metadata);
        _range = checkNotNull(byteRange, "byteRange");
        _streamSupplier = checkNotNull(streamSupplier, "streamSupplier");
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
