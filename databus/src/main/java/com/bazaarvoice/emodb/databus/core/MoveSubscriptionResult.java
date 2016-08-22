package com.bazaarvoice.emodb.databus.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class MoveSubscriptionResult {

    private final Date _moveCompleteTime;

    @JsonCreator
    public MoveSubscriptionResult(@JsonProperty ("moveCompleteTime") Date moveCompleteTime) {
        _moveCompleteTime = moveCompleteTime;
    }

    public Date getMoveCompleteTime() {
        return _moveCompleteTime;
    }
}
