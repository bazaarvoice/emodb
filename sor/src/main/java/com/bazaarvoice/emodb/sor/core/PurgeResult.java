package com.bazaarvoice.emodb.sor.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class PurgeResult {

    private final Date _purgeCompleteTime;

    @JsonCreator
    public PurgeResult(@JsonProperty("purgeCompleteTime") Date purgeCompleteTime) {
        _purgeCompleteTime = purgeCompleteTime;
    }

    public Date getPurgeCompleteTime() {
        return _purgeCompleteTime;
    }
}

