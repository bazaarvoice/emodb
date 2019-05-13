package com.bazaarvoice.emodb.web.scanner.writer;

import com.google.common.base.Optional;
import com.google.common.io.Closeables;
import java.io.IOException;
import java.util.Map;

public abstract class ScanDestinationWriter {

    abstract public void writeDocument(Map<String, Object> document) throws IOException;

    abstract public void closeAndCancel();

    abstract public void closeAndTransferAysnc(Optional<Integer> finalPartCount) throws IOException;

}
