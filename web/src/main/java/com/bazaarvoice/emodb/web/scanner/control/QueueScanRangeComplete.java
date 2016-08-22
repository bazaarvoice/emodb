package com.bazaarvoice.emodb.web.scanner.control;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Implementation of ScanRangeComplete used by {@link QueueScanWorkflow}.  This implementation includes the
 * messageId from the queue entry associated with the work item.
 */
public class QueueScanRangeComplete implements ScanRangeComplete {

    private final String _scanId;
    private String _messageId;

    @JsonCreator
    public QueueScanRangeComplete(@JsonProperty ("scanId") String scanId) {
        _scanId = scanId;
    }

    @Override
    public String getScanId() {
        return _scanId;
    }

    @JsonIgnore
    public String getMessageId() {
        return _messageId;
    }

    public void setMessageId(String messageId) {
        _messageId = messageId;
    }
}
