package com.bazaarvoice.emodb.web.scanner.writer;

import com.google.common.base.Optional;
import java.io.IOException;
import java.util.Map;

public interface ScanDestinationWriter {

    void writeDocument(Map<String, Object> document) throws IOException, InterruptedException;

    void closeAndCancel();

    void closeAndTransferAsync(Optional<Integer> finalPartCount) throws IOException;

}
