package com.bazaarvoice.emodb.web.scanner.control;

/**
 * Message object used by ScanWorkflow to signal that a scan range has completed uploading.  The message content
 * itself contains only the scan ID; the caller must load the scan status to determine which scans completed and
 * whether they were successful.
 */
public interface ScanRangeComplete {

    String getScanId();
}
