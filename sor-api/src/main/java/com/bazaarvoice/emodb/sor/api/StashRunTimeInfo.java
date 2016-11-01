package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ISO8601Utils;

import java.util.Date;
import java.util.List;

public class StashRunTimeInfo {

    private Long _timestamp;
    private String _timestampISO;
    private List<String> _placements;
    private String _dataCenter;
    private Long _expiredTimestamp;
    private String _expiredTimestampISO;

    public StashRunTimeInfo() {}

    public StashRunTimeInfo(Long timestamp, List<String> placements, String dataCenter, Long expiredTimestamp) {
        this._timestamp = timestamp;
        this._timestampISO = (_timestamp != null) ? ISO8601Utils.format(new Date(_timestamp), true) : null;
        this._placements = placements;
        this._dataCenter = dataCenter;
        this._expiredTimestamp = expiredTimestamp;
        this._expiredTimestampISO = (_expiredTimestamp != null) ? ISO8601Utils.format(new Date(_expiredTimestamp), true) : null;
    }

    @JsonCreator
    public StashRunTimeInfo(@JsonProperty ("timestamp") Long timestamp, @JsonProperty ("timestampISO") String timestampISO, @JsonProperty ("placements") List<String> placements,
                            @JsonProperty ("dataCenter") String dataCenter, @JsonProperty ("expiredTimestamp") Long expiredTimestamp, @JsonProperty ("expiredTimestampISO") String expiredTimestampISO) {
        this._timestamp = timestamp;
        this._timestampISO = timestampISO;
        this._placements = placements;
        this._dataCenter = dataCenter;
        this._expiredTimestamp = expiredTimestamp;
        this._expiredTimestampISO = expiredTimestampISO;
    }

    public Long getTimestamp() {
        return _timestamp;
    }

    public String getTimestampISO() {
        return _timestampISO;
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

    public String getExpiredTimestampISO() {
        return _expiredTimestampISO;
    }

    public String toString() {
        try {
            return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }
}
