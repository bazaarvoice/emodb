package com.bazaarvoice.emodb.databus.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class ReplaySubscriptionResult {
    private final Date _replayCompleteTime;

    @JsonCreator
    public ReplaySubscriptionResult(@JsonProperty("replayCompleteTime") Date replayCompleteTime) {
        _replayCompleteTime = replayCompleteTime;
    }

    public Date getReplayCompleteTime() {
        return _replayCompleteTime;
    }
}
