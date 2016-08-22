package com.bazaarvoice.emodb.queue.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class MoveQueueResult {

    private final Date _moveCompleteTime;

    @JsonCreator
    public MoveQueueResult(@JsonProperty ("moveCompleteTime") Date moveCompleteTime) {
        _moveCompleteTime = moveCompleteTime;
    }

    public Date getMoveCompleteTime() {
        return _moveCompleteTime;
    }
}
