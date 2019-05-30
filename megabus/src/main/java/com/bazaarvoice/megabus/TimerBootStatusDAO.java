package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.google.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

public class TimerBootStatusDAO implements BootStatusDAO {

    private final Clock _clock;
    private final Instant _instantiationTime;

    @Inject
    public TimerBootStatusDAO(Clock clock, KafkaCluster kafkaCluster, @MegabusTopic Topic megabusTopic) {
        _clock = checkNotNull(clock, "clock");
        _instantiationTime = clock.instant();

        kafkaCluster.createTopicIfNotExists(megabusTopic);
    }

    @Override
    public boolean isBootComplete() {
        return _instantiationTime.plusSeconds(20).isBefore(_clock.instant());
    }

    @Override
    public void completeBoot() {
        
    }
}
