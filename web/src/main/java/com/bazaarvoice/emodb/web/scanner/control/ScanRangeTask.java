package com.bazaarvoice.emodb.web.scanner.control;

import com.bazaarvoice.emodb.sor.db.ScanRange;

/**
 * Message object used by ScanWorkflow to signal that a scan range is available for uploading.  The message content
 * itself contains only the scan ID, placement and range; the caller must load the scan status to get the scan details,
 * such as to where the data should be uploaded.
 */
public interface ScanRangeTask {
    int getId();

    String getScanId();

    String getPlacement();

    ScanRange getRange();
}
