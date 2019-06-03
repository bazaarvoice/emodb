package com.bazaarvoice.megabus;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class MegabusBootConfiguration {

    private static final int DEFAULT_SCAN_THREAD_COUNT = 8;


    // the API key to use for EmoDB queues
    @Valid
    @NotNull
    @JsonProperty("queueServiceApiKey")
    private String _queueServiceApiKey;

    // Name of the table which holds scan status entries.
    @Valid
    @NotNull
    @JsonProperty ("scanStatusTable")
    private String _scanStatusTable;

    @Valid
    @NotNull
    @JsonProperty ("pendingScanRangeQueueName")
    private String _pendingScanRangeQueueName;

    @Valid
    @NotNull
    @JsonProperty ("completeScanRangeQueueName")
    private String _completeScanRangeQueueName;

    // Maximum number of scan threads that can run concurrently on a single server.  Default is 8.
    @Valid
    @NotNull
    @JsonProperty ("scanThreadCount")
    private int _scanThreadCount = DEFAULT_SCAN_THREAD_COUNT;

    public String getQueueServiceApiKey() {
        return _queueServiceApiKey;
    }

    public String getScanStatusTable() {
        return _scanStatusTable;
    }

    public String getPendingScanRangeQueueName() {
        return _pendingScanRangeQueueName;
    }

    public String getCompleteScanRangeQueueName() {
        return _completeScanRangeQueueName;
    }

    public int getScanThreadCount() {
        return _scanThreadCount;
    }
}
