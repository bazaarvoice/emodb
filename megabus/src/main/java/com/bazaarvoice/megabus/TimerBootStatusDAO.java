package com.bazaarvoice.megabus;

import com.google.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

public class TimerBootStatusDAO implements BootStatusDAO {

    private final Clock _clock;
    private final Instant _instantiationTime;

    @Inject
    public TimerBootStatusDAO(Clock clock) {
        _clock = checkNotNull(clock, "clock");
        _instantiationTime = clock.instant();
    }

    @Override
    public boolean isBootComplete() {
        return _instantiationTime.plusSeconds(20).isBefore(_clock.instant());
    }

    @Override
    public void completeBoot() {
        
    }
}
