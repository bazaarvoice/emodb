package com.bazaarvoice.emodb.databus.core;

import javax.validation.constraints.NotNull;
import java.sql.Time;

public final class FannedOutUpdateRetryRef {

    @NotNull private final FannedOutUpdateRef _updateRef;
    @NotNull private final Long _timeOfLastResolve;
    @NotNull private final Long _timeOfFirstResolve;

    public FannedOutUpdateRetryRef(@NotNull FannedOutUpdateRef updateRef, @NotNull Long timeOfFirstResolve, @NotNull Long timeOfLastResolve) {
        _updateRef = updateRef;
        _timeOfLastResolve = timeOfLastResolve;
        _timeOfFirstResolve = timeOfFirstResolve;
    }

    public FannedOutUpdateRef getFannedOutUpdateRef() { return _updateRef; }

    public Long getTimeOfLastResolve() { return _timeOfLastResolve; }

    public Long getTimeOfFirstResolve() { return _timeOfFirstResolve; }

}
