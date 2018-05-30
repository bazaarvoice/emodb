package com.bazaarvoice.emodb.sor.api.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class TableReportMetadata {
    private final String _id;
    private final Date _startTime;
    private final Date _endTime;
    private final Boolean _success;
    private final List<String> _placements;

    @JsonCreator
    public TableReportMetadata(@JsonProperty("id") String id,
                               @JsonProperty("startTime") Date startTime,
                               @JsonProperty("endTime") @Nullable Date endTime,
                               @JsonProperty("success") @Nullable Boolean success,
                               @JsonProperty("placements") List<String> placements) {
        _id = requireNonNull(id, "id");
        _startTime = requireNonNull(startTime, "startTime");
        _endTime = endTime;
        _success = success;
        _placements = requireNonNull(placements, "placements");
    }

    public String getId() {
        return _id;
    }

    public Date getStartTime() {
        return _startTime;
    }

    public Date getEndTime() {
        return _endTime;
    }

    public Boolean getSuccess() {
        return _success;
    }

    public List<String> getPlacements() {
        return _placements;
    }
}
