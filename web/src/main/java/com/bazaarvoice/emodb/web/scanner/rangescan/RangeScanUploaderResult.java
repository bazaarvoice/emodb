package com.bazaarvoice.emodb.web.scanner.rangescan;

import com.bazaarvoice.emodb.sor.db.ScanRange;

import javax.annotation.Nullable;

/**
 * Signals to the caller the result of a range scan operation.  This can be one of three values:
 * <ol>
 *     <li>Success:  Operation completed normally
 *     <li>Failure:  Operation failed and needs to be completely retried
 *     <li>Resplit:  Operation partially completed.  A non-zero portion of the range was scanned and uploaded
 *                   successfully but there remains more rows that were not uploaded.  The portion of the range
 *                   that needs to be resplit and rescheduled can be retrieved using {@link #getResplitRange()}
 * </ol>
 *
 */
public class RangeScanUploaderResult {

    public enum Status {
        SUCCESS,
        FAILURE,
        REPSPLIT
    }

    private final static RangeScanUploaderResult SUCCESS_INSTANCE = new RangeScanUploaderResult(Status.SUCCESS, null);
    private final static RangeScanUploaderResult FAILURE_INSTANCE = new RangeScanUploaderResult(Status.FAILURE, null);

    public static RangeScanUploaderResult success() {
        return SUCCESS_INSTANCE;
    }

    public static RangeScanUploaderResult failure() {
        return FAILURE_INSTANCE;
    }

    public static RangeScanUploaderResult resplit(ScanRange resplitRange) {
        return new RangeScanUploaderResult(Status.REPSPLIT, resplitRange);
    }

    private final Status _status;
    private final ScanRange _resplitRange;

    public RangeScanUploaderResult(Status status, @Nullable ScanRange resplitRange) {
        _status = status;
        _resplitRange = resplitRange;
    }

    public Status getStatus() {
        return _status;
    }

    @Nullable
    public ScanRange getResplitRange() {
        return _resplitRange;
    }

    @Override
    public String toString() {
        return _status.toString();
    }
}
