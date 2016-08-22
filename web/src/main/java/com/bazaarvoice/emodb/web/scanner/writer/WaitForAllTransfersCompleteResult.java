package com.bazaarvoice.emodb.web.scanner.writer;

import java.util.Map;

/**
 * Simple object for the results of {@link TemporaryFileScanWriter#waitForAllTransfersComplete(org.joda.time.Duration)}.
 */
public class WaitForAllTransfersCompleteResult {
    private final Map<TransferKey, TransferStatus> _statusMap;

    public WaitForAllTransfersCompleteResult(Map<TransferKey, TransferStatus> statusMap) {
        _statusMap = statusMap;
    }

    public Map<TransferKey, TransferStatus> getActiveTransferStatusMap() {
        return _statusMap;
    }

    public boolean isComplete() {
        return _statusMap.isEmpty();
    }
}
