package com.bazaarvoice.emodb.web.migrator.migratorstatus;


import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.*;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.MapDeltaBuilder;
import com.bazaarvoice.emodb.web.migrator.MigratorRateLimiter;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.scanstatus.*;
import com.google.common.base.Optional;
import com.google.common.collect.*;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MigratorStatusDAO implements MigratorRateLimiter {

    private final DataStore _dataStore;
    private final String _tableName;
    private final String _tablePlacement;
    private volatile boolean _tableChecked = false;

    @Inject
    public MigratorStatusDAO(DataStore dataStore,
                                  @MigratorStatusTable String tableName,
                                  @MigratorStatusTablePlacement String tablePlacement) {
        _dataStore = dataStore;
        _tableName = tableName;
        _tablePlacement = tablePlacement;
    }

    /**
     * Returns the migration status table name.  On the first call it also verifies that the table exists, then skips this
     * check on future calls.
     */
    private String getTable() {
        if (!_tableChecked) {
            if (!_dataStore.getTableExists(_tableName)) {
                _dataStore.createTable(
                        _tableName,
                        new TableOptionsBuilder().setPlacement(_tablePlacement).build(),
                        ImmutableMap.<String, Object>of(),
                        new AuditBuilder().setLocalHost().setComment("Create migration status table").build());

                _tableChecked = true;
            }
        }

        return _tableName;
    }

    public Iterator<MigratorStatus> list(@Nullable String fromIdExclusive, long limit) {
        return Iterators.transform(
                _dataStore.scan(getTable(), fromIdExclusive, limit, ReadConsistency.STRONG),
                        map -> fromMap(map));
    }

    public void updateMigratorStatus(MigratorStatus status) {
        Map<String, Object> ranges = Maps.newHashMap();

        for (ScanRangeStatus rangeStatus :
                Iterables.concat(status.getPendingScanRanges(), status.getActiveScanRanges(), status.getCompleteScanRanges())) {

            Map<String, Object> rangeMap = migratorRangeStatusToMap(rangeStatus);
            ranges.put(toRangeKey(rangeStatus.getTaskId()), rangeMap);
        }

        _dataStore.update(getTable(), status.getScanId(), TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .put("options", JsonHelper.convert(status.getOptions(), Map.class))
                        .put("ranges", ranges)
                        .put("canceled", false)
                        .put("startTime", status.getStartTime().getTime())
                        .put("completeTime", status.getCompleteTime() != null ? status.getCompleteTime().getTime() : null)
                        .put("maxWritesPerSecond", status.getMaxWritesPerSecond())
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Starting migration").build());
    }

    private Map<String, Object> migratorRangeStatusToMap(ScanRangeStatus rangeStatus) {
        ImmutableMap.Builder<String, Object> rangeMap = ImmutableMap.<String, Object>builder()
                .put("taskId", rangeStatus.getTaskId())
                .put("placement", rangeStatus.getPlacement())
                .put("range", JsonHelper.convert(rangeStatus.getScanRange(), Map.class))
                .put("batch", rangeStatus.getBatchId());

        if (rangeStatus.getBlockedByBatchId().isPresent()) {
            rangeMap.put("blockedByBatch", rangeStatus.getBlockedByBatchId().get());
        }
        if (rangeStatus.getConcurrencyId().isPresent()) {
            rangeMap.put("concurrencyGroup", rangeStatus.getConcurrencyId().get());
        }

        if (rangeStatus.getScanQueuedTime() != null) {
            rangeMap.put("queuedTime", rangeStatus.getScanQueuedTime().getTime());

            if (rangeStatus.getScanStartTime() != null) {
                rangeMap.put("startTime", rangeStatus.getScanStartTime().getTime());

                if (rangeStatus.getScanCompleteTime() != null) {
                    rangeMap.put("completeTime", rangeStatus.getScanCompleteTime().getTime());

                    if (rangeStatus.getResplitRange().isPresent()) {
                        rangeMap.put("resplitRange", JsonHelper.convert(rangeStatus.getResplitRange().get(), Map.class));
                    }
                }
            }
        }

        return rangeMap.build();
    }

    public MigratorStatus getMigratorStatus(String migrationId) {
        Map<String, Object> map = _dataStore.get(getTable(), migrationId);
        return fromMap(map);
    }

    private MigratorStatus fromMap(Map<String, Object> map) {
        if (Intrinsic.isDeleted(map)) {
            return null;
        }
        
        boolean canceled = (Boolean) map.get("canceled");
        ScanOptions options = JsonHelper.convert(map.get("options"), ScanOptions.class);
        Long completeTs = (Long) map.get("completeTime");
        Date completeTime = completeTs != null ? new Date(completeTs) : null;
        int maxWritesPerSecond = (Integer) map.get("maxWritesPerSecond");

        //noinspection unchecked
        Map<String, Object> ranges = (Map<String, Object>) map.get("ranges");

        List<ScanRangeStatus> pendingMigrationRanges = Lists.newArrayList();
        List<ScanRangeStatus> activeMigrationRanges = Lists.newArrayList();
        List<ScanRangeStatus> completeMigrationRanges = Lists.newArrayList();

        for (Object rangeObject : ranges.values()) {
            //noinspection unchecked
            Map<String, Object> rangeJson = (Map<String, Object>) rangeObject;
            int taskId = (Integer) rangeJson.get("taskId");
            String placement = (String) rangeJson.get("placement");
            ScanRange range = JsonHelper.convert(rangeJson.get("range"), ScanRange.class);
            int batchId = (Integer) rangeJson.get("batch");
            Optional<Integer> blockedByBatchId = Optional.fromNullable((Integer) rangeJson.get("blockedByBatch"));
            Optional<Integer> concurrencyId = Optional.fromNullable(((Integer) rangeJson.get("concurrencyGroup")));
            ScanRangeStatus migrationRangeStatus = new ScanRangeStatus(taskId, placement, range, batchId, blockedByBatchId, concurrencyId);

            Long time = (Long) rangeJson.get("queuedTime");
            if (time != null) {
                migrationRangeStatus.setScanQueuedTime(new Date(time));
            }

            time = (Long) rangeJson.get("startTime");
            if (time == null) {
                // Not yet started, so this range is pending
                pendingMigrationRanges.add(migrationRangeStatus);
            } else {
                migrationRangeStatus.setScanStartTime(new Date(time));
                time = (Long) rangeJson.get("completeTime");
                if (time == null) {
                    // Not yet complete
                    activeMigrationRanges.add(migrationRangeStatus);
                } else {
                    migrationRangeStatus.setScanCompleteTime(new Date(time));
                    migrationRangeStatus.setResplitRange(JsonHelper.convert(rangeJson.get("resplitRange"), ScanRange.class));
                    completeMigrationRanges.add(migrationRangeStatus);
                }
            }
        }

        Date startTime = new Date((Long) map.get("startTime"));

        return new MigratorStatus(Intrinsic.getId(map), options, canceled, startTime, pendingMigrationRanges, activeMigrationRanges,
                completeMigrationRanges, completeTime, maxWritesPerSecond);
    }

    public void setMigratorRangeTaskQueued(String migrationId, int taskId, Date queuedTime) {
        _dataStore.update(getTable(), migrationId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("ranges", Deltas.mapBuilder()
                                .updateIfExists(toRangeKey(taskId), Deltas.mapBuilder()
                                        .putIfAbsent("queuedTime", queuedTime.getTime())
                                        .build())
                                .build())
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Migration range queued").build());
    }

    public void setMigratorRangeTaskActive(String migrationId, int taskId, Date startTime) {
        _dataStore.update(getTable(), migrationId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("ranges", Deltas.mapBuilder()
                                .updateIfExists(toRangeKey(taskId), Deltas.mapBuilder()
                                        .putIfAbsent("startTime", startTime.getTime())
                                        .build())
                                .build())
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Migration range started").build());
    }

    public void setMigratorRangeTaskInactive(String migrationId, int taskId) {
        _dataStore.update(getTable(), migrationId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("ranges", Deltas.mapBuilder()
                                .updateIfExists(toRangeKey(taskId), Deltas.mapBuilder()
                                        .remove("queuedTime")
                                        .remove("startTime")
                                        .remove("completeTime")
                                        .build())
                                .build())
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Migration range inactive").build());
    }

    public void setMigratorRangeTaskComplete(String migrationId, int taskId, Date completeTime) {
        _dataStore.update(getTable(), migrationId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("ranges", Deltas.mapBuilder()
                                .updateIfExists(toRangeKey(taskId), Deltas.mapBuilder()
                                        .putIfAbsent("completeTime", completeTime.getTime())
                                        .build())
                                .build())
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Migration range complete").build());
    }

    public void setMigratorRangeTaskPartiallyComplete(String migrationId, int taskId, ScanRange completeRange, ScanRange resplitRange, Date completeTime) {
        _dataStore.update(getTable(), migrationId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("ranges", Deltas.mapBuilder()
                                .updateIfExists(toRangeKey(taskId), Deltas.mapBuilder()
                                        .putIfAbsent("completeTime", completeTime.getTime())
                                        .put("range", JsonHelper.convert(completeRange, Map.class))
                                        .put("resplitRange", JsonHelper.convert(resplitRange, Map.class))
                                        .build())
                                .build())
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Scan range partially complete").build());
    }

    public void resplitMigratorRangeTask(String migrationId, int taskId, List<ScanRangeStatus> splitMigrationStatuses) {
        MapDeltaBuilder rangeBuilder = Deltas.mapBuilder();

        // Update the original range migration to remove the resplit request
        rangeBuilder.update(toRangeKey(taskId), Deltas.mapBuilder()
                .remove("resplitRange")
                .build());

        // Add all of the new split migration ranges
        for (ScanRangeStatus splitMigrationStatus : splitMigrationStatuses) {
            Map<String, Object> splitMigrationMap = migratorRangeStatusToMap(splitMigrationStatus);
            rangeBuilder.update(toRangeKey(splitMigrationStatus.getTaskId()), Deltas.literal(splitMigrationMap));
        }

        _dataStore.update(getTable(), migrationId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("ranges", rangeBuilder.build())
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Migration range resplit").build());
    }

    public void setCompleteTime(String migrationId, Date completeTime) {
        _dataStore.update(getTable(), migrationId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("completeTime", Deltas.conditional(Conditions.isNull(), Deltas.literal(completeTime.getTime())))
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Migration complete").build());
    }

    public void setCanceled(String migrationId) {
        _dataStore.update(getTable(), migrationId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("canceled", Deltas.conditional(Conditions.equal(false), Deltas.literal(true)))
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Canceling migration").build());
    }

    @Override
    public void setMaxWritesPerSecond(String migrationId, int maxWritesPerSecond) {
        _dataStore.update(getTable(), migrationId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
        .put("maxWritesPerSecond", maxWritesPerSecond).build(),
                new AuditBuilder().setLocalHost().setComment("modifying maxWritesPerSecond").build());
    }

    @Override
    public int getMaxWritesPerSecond(String migrationId) {
        Map<String, Object> status = _dataStore.get(getTable(), migrationId);
        return (int) status.get("maxWritesPerSecond");
    }

    private String toRangeKey(int taskId) {
        return String.valueOf(taskId);
    }
}
