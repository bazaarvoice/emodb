package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.google.common.base.Objects;

import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * POJO for metadata about a request for a scan.
 */
public class ScanRequest {

    private final String _requestedBy;
    private final Date _requestTime;

    public ScanRequest(String requestedBy, Date requestTime) {
        _requestedBy = checkNotNull(requestedBy, "requestedBy");
        _requestTime = checkNotNull(requestTime, "requestTime");
    }

    public String getRequestedBy() {
        return _requestedBy;
    }

    public Date getRequestTime() {
        return _requestTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ScanRequest)) {
            return false;
        }

        ScanRequest that = (ScanRequest) o;

        return _requestedBy.equals(that._requestedBy) &&
                _requestTime.equals(that._requestTime);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_requestedBy, _requestTime);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("requestedBy", _requestedBy)
                .add("requestTime", _requestTime)
                .toString();
    }
}
