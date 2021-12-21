package com.bazaarvoice.emodb.web.scanner.writer;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public interface ScanDestinationWriter {

    void writeDocument(Map<String, Object> document) throws IOException, InterruptedException;

    void closeAndCancel();

    void closeAndTransferAsync(Optional<Integer> finalPartCount) throws IOException;

}
