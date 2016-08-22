package com.bazaarvoice.emodb.blob.api;

import java.io.IOException;
import java.io.OutputStream;

public interface StreamSupplier {

    void writeTo(OutputStream out) throws IOException;
}
