package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.MapDeltaBuilder;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * ScanStatusDAO implementation that persists to an EmoDB table.
 */
public class DataStoreScanStatusDAO implements ScanStatusDAO {

    private final DataStore _dataStore;
    private final String _tableName;
    private final String _tablePlacement;
    private volatile boolean _tableChecked = false;

    @Inject
    public DataStoreScanStatusDAO(DataStore dataStore,
                                  @ScanStatusTable String tableName,
                                  @ScanStatusTablePlacement String tablePlacement) {
        _dataStore = dataStore;
        _tableName = tableName;
        _tablePlacement = tablePlacement;
    }

    /**
     * Returns the scan status table name.  On the first call it also verifies that the table exists, then skips this
     * check on future calls.
     */
    private String getTable() {
        if (!_tableChecked) {
            if (!_dataStore.getTableExists(_tableName)) {
                _dataStore.createTable(
                        _tableName,
                        new TableOptionsBuilder().setPlacement(_tablePlacement).build(),
                        ImmutableMap.<String, Object>of(),
                        new AuditBuilder().setLocalHost().setComment("Create scan status table").build());

                _tableChecked = true;
            }
        }

        return _tableName;
    }

    @Override
    public Iterator<ScanStatus> list(@Nullable String fromIdExclusive, long limit) {
        return Iterators.transform(
                _dataStore.scan(getTable(), fromIdExclusive, limit, ReadConsistency.STRONG),
                new Function<Map<String, Object>, ScanStatus>() {
                    @Override
                    public ScanStatus apply(Map<String, Object> map) {
                        return fromMap(map);
                    }
                });
    }

    @Override
    public void updateScanStatus(ScanStatus status) {
        Map<String, Object> ranges = Maps.newHashMap();

        for (ScanRangeStatus rangeStatus :
                Iterables.concat(status.getPendingScanRanges(), status.getActiveScanRanges(), status.getCompleteScanRanges())) {

            Map<String, Object> rangeMap = scanRangeStatusToMap(rangeStatus);
            ranges.put(toRangeKey(rangeStatus.getTaskId()), rangeMap);
        }

        _dataStore.update(getTable(), status.getScanId(), TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .put("options", JsonHelper.convert(status.getOptions(), Map.class))
                        .put("ranges", ranges)
                        .put("canceled", false)
                        .put("startTime", status.getStartTime().getTime())
                        .put("completeTime", status.getCompleteTime() != null ? status.getCompleteTime().getTime() : null)
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Starting scan").build());
    }

    private Map<String, Object> scanRangeStatusToMap(ScanRangeStatus rangeStatus) {
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

    @Override
    public ScanStatus getScanStatus(String scanId) {
        Map<String, Object> map = _dataStore.get(getTable(), scanId);
        return fromMap(map);
    }

    private ScanStatus fromMap(Map<String, Object> map) {
        if (Intrinsic.isDeleted(map)) {
            return null;
        }

        boolean canceled = (Boolean) map.get("canceled");
        ScanOptions options = JsonHelper.convert(map.get("options"), ScanOptions.class);
        Long completeTs = (Long) map.get("completeTime");
        Date completeTime = completeTs != null ? new Date(completeTs) : null;

        //noinspection unchecked
        Map<String, Object> ranges = (Map<String, Object>) map.get("ranges");

        List<ScanRangeStatus> pendingScanRanges = Lists.newArrayList();
        List<ScanRangeStatus> activeScanRanges = Lists.newArrayList();
        List<ScanRangeStatus> completeScanRanges = Lists.newArrayList();

        for (Object rangeObject : ranges.values()) {
            //noinspection unchecked
            Map<String, Object> rangeJson = (Map<String, Object>) rangeObject;
            int taskId = (Integer) rangeJson.get("taskId");
            String placement = (String) rangeJson.get("placement");
            ScanRange range = JsonHelper.convert(rangeJson.get("range"), ScanRange.class);
            int batchId = (Integer) rangeJson.get("batch");
            Optional<Integer> blockedByBatchId = Optional.fromNullable((Integer) rangeJson.get("blockedByBatch"));
            Optional<Integer> concurrencyId = Optional.fromNullable(((Integer) rangeJson.get("concurrencyGroup")));
            ScanRangeStatus scanRangeStatus = new ScanRangeStatus(taskId, placement, range, batchId, blockedByBatchId, concurrencyId);

            Long time = (Long) rangeJson.get("queuedTime");
            if (time != null) {
                scanRangeStatus.setScanQueuedTime(new Date(time));
            }

            time = (Long) rangeJson.get("startTime");
            if (time == null) {
                // Not yet started, so this range is pending
                pendingScanRanges.add(scanRangeStatus);
            } else {
                scanRangeStatus.setScanStartTime(new Date(time));
                time = (Long) rangeJson.get("completeTime");
                if (time == null) {
                    // Not yet complete
                    activeScanRanges.add(scanRangeStatus);
                } else {
                    scanRangeStatus.setScanCompleteTime(new Date(time));
                    scanRangeStatus.setResplitRange(JsonHelper.convert(rangeJson.get("resplitRange"), ScanRange.class));
                    completeScanRanges.add(scanRangeStatus);
                }
            }
        }

        // Early versions of ScanStatus did not include a startTime attribute.  If none is found extrapolate one.
        Long startTs = (Long) map.get("startTime");
        Date startTime = startTs != null ?
                new Date(startTs) :
                extrapolateStartTimeFromScanRanges(pendingScanRanges, activeScanRanges, completeScanRanges);

        return new ScanStatus(Intrinsic.getId(map), options, canceled, startTime, pendingScanRanges, activeScanRanges,
                completeScanRanges, completeTime);
    }

    /**
     * For grandfathered in ScanStatuses that did not include a startTime attribute extrapolate it as the earliest
     * time a scan range was queued.
     */
    private Date extrapolateStartTimeFromScanRanges(List<ScanRangeStatus> pendingScanRanges,
                                                    List<ScanRangeStatus> activeScanRanges,
                                                    List<ScanRangeStatus> completeScanRanges) {
        Date startTime = null;

        for (ScanRangeStatus status : Iterables.concat(pendingScanRanges, activeScanRanges, completeScanRanges)) {
            Date queuedTime = status.getScanQueuedTime();
            if (queuedTime != null && (startTime == null || queuedTime.before(startTime))) {
                startTime = queuedTime;
            }
        }

        if (startTime == null) {
            // Either the scan contained no scan ranges or no scans were ever queued, neither of which is likely to
            // ever happen.  But, so we don't return null choose a reasonably old date.
            startTime = new Date(0);
        }

        return startTime;
    }

    @Override
    public void setScanRangeTaskQueued(String scanId, int taskId, Date queuedTime) {
        _dataStore.update(getTable(), scanId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("ranges", Deltas.mapBuilder()
                                .updateIfExists(toRangeKey(taskId), Deltas.mapBuilder()
                                        .putIfAbsent("queuedTime", queuedTime.getTime())
                                        .build())
                                .build())
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Scan range queued").build());
    }

    @Override
    public void setScanRangeTaskActive(String scanId, int taskId, Date startTime) {
        _dataStore.update(getTable(), scanId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("ranges", Deltas.mapBuilder()
                                .updateIfExists(toRangeKey(taskId), Deltas.mapBuilder()
                                        .putIfAbsent("startTime", startTime.getTime())
                                        .build())
                                .build())
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Scan range started").build());
    }

    @Override
    public void setScanRangeTaskInactive(String scanId, int taskId) {
        _dataStore.update(getTable(), scanId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("ranges", Deltas.mapBuilder()
                                .updateIfExists(toRangeKey(taskId), Deltas.mapBuilder()
                                        .remove("queuedTime")
                                        .remove("startTime")
                                        .remove("completeTime")
                                        .build())
                                .build())
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Scan range inactive").build());
    }

    @Override
    public void setScanRangeTaskComplete(String scanId, int taskId, Date completeTime) {
        _dataStore.update(getTable(), scanId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("ranges", Deltas.mapBuilder()
                                .updateIfExists(toRangeKey(taskId), Deltas.mapBuilder()
                                        .putIfAbsent("completeTime", completeTime.getTime())
                                        .build())
                                .build())
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Scan range complete").build());
    }

    @Override
    public void setScanRangeTaskPartiallyComplete(String scanId, int taskId, ScanRange completeRange, ScanRange resplitRange, Date completeTime) {
        _dataStore.update(getTable(), scanId, TimeUUIDs.newUUID(),
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

    @Override
    public void resplitScanRangeTask(String scanId, int taskId, List<ScanRangeStatus> splitScanStatuses) {
        MapDeltaBuilder rangeBuilder = Deltas.mapBuilder();

        // Update the original range scan to remove the resplit request
        rangeBuilder.update(toRangeKey(taskId), Deltas.mapBuilder()
                .remove("resplitRange")
                .build());

        // Add all of the new split scan ranges
        for (ScanRangeStatus splitScanStatus : splitScanStatuses) {
            Map<String, Object> splitScanMap = scanRangeStatusToMap(splitScanStatus);
            rangeBuilder.update(toRangeKey(splitScanStatus.getTaskId()), Deltas.literal(splitScanMap));
        }

        _dataStore.update(getTable(), scanId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("ranges", rangeBuilder.build())
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Scan range resplit").build());
    }

    @Override
    public void setCompleteTime(String scanId, Date completeTime) {
        _dataStore.update(getTable(), scanId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("completeTime", Deltas.conditional(Conditions.isNull(), Deltas.literal(completeTime.getTime())))
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Scan complete").build());
    }

    @Override
    public void setCanceled(String scanId) {
        _dataStore.update(getTable(), scanId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("canceled", Deltas.conditional(Conditions.equal(false), Deltas.literal(true)))
                        .build(),
                new AuditBuilder().setLocalHost().setComment("Canceling scan").build());
    }

    private String toRangeKey(int taskId) {
        return String.valueOf(taskId);
    }
}
