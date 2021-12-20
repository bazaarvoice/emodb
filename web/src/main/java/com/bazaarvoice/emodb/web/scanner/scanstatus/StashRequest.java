package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.Date;

import static java.util.Objects.requireNonNull;

/**
 * POJO for metadata about a request for a scan.
 */
public class StashRequest {

    private final String _requestedBy;
    private final Date _requestTime;

    public StashRequest(String requestedBy, Date requestTime) {
        _requestedBy = requireNonNull(requestedBy, "requestedBy");
        _requestTime = requireNonNull(requestTime, "requestTime");
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
        if (!(o instanceof StashRequest)) {
            return false;
        }

        StashRequest that = (StashRequest) o;

        return _requestedBy.equals(that._requestedBy) &&
                _requestTime.equals(that._requestTime);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_requestedBy, _requestTime);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("requestedBy", _requestedBy)
                .add("requestTime", _requestTime)
                .toString();
    }
}
