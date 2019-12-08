package com.bazaarvoice.emodb.table.db.eventregistry;

import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.table.db.TableBackingStore;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import javax.annotation.Nullable;
import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultTableEventRegistry implements TableEventRegistry {

    private static final String TABLE_NAME = "";

    private final DataCenters _dataCenters;
    private final Clock _clock;
    private final TableBackingStore _tableBackingStore;
    private final String _hostAndPort;

    @Inject
    public DefaultTableEventRegistry(DataCenters dataCenters,
                                     @Nullable Clock clock,
                                     TableBackingStore tableBackingStore,
                                     @SelfHostAndPort HostAndPort hostAndPort) {
        _dataCenters = checkNotNull(dataCenters);
        _clock = Objects.firstNonNull(clock, Clock.systemUTC());
        _tableBackingStore = checkNotNull(tableBackingStore);
        _hostAndPort = checkNotNull(hostAndPort).toString();

    }

    public void registerTableListener(String registrationId, Instant newExpirationTime) {
        Instant now = _clock.instant();
        Condition isExpired = Conditions.mapBuilder().matches("expirationTime", Conditions.le(now.toEpochMilli())).build();

        Delta registrationDelta = Deltas.mapBuilder()
                .update(registrationId, Deltas.mapBuilder()
                        .put("expirationTime", newExpirationTime.toEpochMilli())
                        .update("tasks", Deltas.conditional(isExpired, Deltas.literal(ImmutableMap.of())))
                        .build())
                .build();

        Audit audit = new AuditBuilder()
                .setComment(String.format("Registering %s", registrationId))
                .setProgram(registrationId)
                .setHost(_hostAndPort)
                .build();

        _tableBackingStore.update(TABLE_NAME, _dataCenters.getSelf().getName(), TimeUUIDs.newUUID(), registrationDelta, audit, WriteConsistency.GLOBAL);
    }

//    public void registerTableEvent(TableEvent tableEvent) {
//        Collection<DataCenter> datacenters = _dataCenters.getAll();
//        Instant now = _clock.instant();
//
//
//        for (DataCenter datacenter : datacenters) {
//            Map<String, Object> datacenterRegistrantMap = _tableBackingStore.get(TABLE_NAME, datacenter.getName(), ReadConsistency.STRONG);
//            datacenterRegistrantMap.keySet();
//            Conditions.mapBuilder().matches("expirationTime", Conditions.ge(now.toEpochMilli())).build();
//            Deltas.set
//            Delta tableEventDelta = Deltas.mapBuilder().updateIfExists(datacenter.getName(), )
//        }
//
//
//        _tableBackingStore.get(TABLE_NAME, datacenter, ReadConsistency.STRONG);
//    }

//    public Object getEventForProcessing() {
//        _tableBackingStore.get(TABLE_NAME, _dataCenters.getSelf(), ReadConsistency.STRONG)
//    }

    public void completeEvent() {

    }

}