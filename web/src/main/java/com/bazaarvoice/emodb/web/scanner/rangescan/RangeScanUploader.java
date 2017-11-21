package com.bazaarvoice.emodb.web.scanner.rangescan;

import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;

import java.io.IOException;

/**
 * Defines the interface for scanning and uploading a scan range.
 */
public interface RangeScanUploader {

    /**
     * Uploads a scan range
     * @param scanId Unique identifier for this scan
     * @param taskId Unique identifier for this scan and upload's placement and range
     * @param scanOptions The options associated with the scan operation
     * @param placement The placement to scan
     * @param scanRange The range to be scanned and uploaded
     */
    RangeScanUploaderResult scanAndUpload(String scanId, int taskId, ScanOptions scanOptions, String placement, ScanRange scanRange)
            throws IOException, InterruptedException;
}
