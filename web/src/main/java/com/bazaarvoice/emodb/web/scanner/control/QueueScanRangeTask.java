package com.bazaarvoice.emodb.web.scanner.control;

import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.util.Objects;

/**
 * Implementation of ScanRangeTask used by {@link QueueScanWorkflow}.  This implementation includes the
 * messageId from the queue entry associated with the work item.
 */
public class QueueScanRangeTask implements ScanRangeTask {

    private final int _id;
    private final String _scanId;
    private final String _placement;
    private final ScanRange _range;
    private String _messageId;

    @JsonCreator
    public QueueScanRangeTask(@JsonProperty ("id") int id,
                              @JsonProperty ("scanId") String scanId,
                              @JsonProperty ("placement") String placement,
                              @JsonProperty ("range") ScanRange range) {
        _id = id;
        _scanId = scanId;
        _placement = placement;
        _range = range;
    }

    @Override
    public int getId() {
        return _id;
    }

    @Override
    public String getScanId() {
        return _scanId;
    }

    @Override
    public String getPlacement() {
        return _placement;
    }

    @Override
    public ScanRange getRange() {
        return _range;
    }

    @JsonIgnore
    public String getMessageId() {
        return _messageId;
    }

    public void setMessageId(String messageId) {
        _messageId = messageId;
    }

    public int hashCode() {
        return _messageId != null ? _messageId.hashCode() : 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof QueueScanRangeTask)) {
            return false;
        }

        QueueScanRangeTask that = (QueueScanRangeTask) o;

        return Objects.equals(_messageId, that.getMessageId()) &&
                _id == that._id &&
                Objects.equals(_scanId, that.getScanId()) &&
                Objects.equals(_range, that.getRange());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", _id)
                .add("scanId", _scanId)
                .add("placement", _placement)
                .add("range", _range)
                .toString();
    }
}
