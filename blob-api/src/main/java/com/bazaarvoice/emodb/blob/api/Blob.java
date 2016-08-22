package com.bazaarvoice.emodb.blob.api;

import java.io.IOException;
import java.io.OutputStream;

public interface Blob extends BlobMetadata, StreamSupplier {

    /**
     * Returns the subset of bytes within the blob that will returned by the {@link #writeTo} method.
     */
    Range getByteRange();

    /**
     * Copies the blob content byte range to the specified output stream.
     */
    void writeTo(OutputStream out) throws IOException;
}
