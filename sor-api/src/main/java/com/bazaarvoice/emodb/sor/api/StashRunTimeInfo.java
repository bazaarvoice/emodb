package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class StashRunTimeInfo {

    private Long _timestamp;
    private List<String> _placements;
    private String _dataCenter;
    private Long _expiredTimestamp;

    public StashRunTimeInfo() {
    }

    @JsonCreator
    public StashRunTimeInfo(@JsonProperty ("timestamp") Long timestamp, @JsonProperty ("placements") List<String> placements,
                            @JsonProperty ("dataCenter") String dataCenter, @JsonProperty ("expiredTimestamp") Long expiredTimestamp) {
        this._timestamp = timestamp;
        this._placements = placements;
        this._dataCenter = dataCenter;
        this._expiredTimestamp = expiredTimestamp;
    }

    public Long getTimestamp() {
        return _timestamp;
    }

    public List<String> getPlacements() {
        return _placements;
    }

    public String getDataCenter() {
        return _dataCenter;
    }

    public Long getExpiredTimestamp() {
        return _expiredTimestamp;
    }

    public String toString() {
        return JsonHelper.asJson(this);
    }
}
