package com.bazaarvoice.emodb.web.report.db;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.api.report.TableReportEntry;
import com.bazaarvoice.emodb.sor.api.report.TableReportEntryTable;
import com.bazaarvoice.emodb.sor.api.report.TableReportMetadata;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.MapDeltaBuilder;
import com.bazaarvoice.emodb.table.db.astyanax.SystemTablePlacement;
import com.bazaarvoice.emodb.web.report.AllTablesReportQuery;
import com.bazaarvoice.emodb.web.report.ReportNotFoundException;
import com.bazaarvoice.emodb.web.report.TableStatistics;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of AllTablesReportDAO that is backed by an EmoDB table.
 */
public class EmoTableAllTablesReportDAO implements AllTablesReportDAO {
    private static final String REPORT_TABLE_PREFIX = "__table_report:";
    private static final String REPORT_METADATA_KEY = "~reportMetadata";

    private final DataStore _dataStore;
    private final String _systemTablePlacement;

    @Inject
    public EmoTableAllTablesReportDAO(DataStore dataStore, @SystemTablePlacement String systemTablePlacement) {
        _dataStore = dataStore;
        _systemTablePlacement = systemTablePlacement;
    }

    /**
     * Verifies that the given report table exists or, if not, creates it.
     */
    @Override
    public void verifyOrCreateReport(String reportId) {
        checkNotNull(reportId, "reportId");

        String tableName = getTableName(reportId);
        if (_dataStore.getTableExists(tableName)) {
            return;  // Nothing to do.
        }

        try {
            // TODO: when run in the EU this should be routed to the US data center.
            _dataStore.createTable(tableName,
                    new TableOptionsBuilder().setPlacement(_systemTablePlacement).build(),
                    ImmutableMap.<String, String>of(),
                    new AuditBuilder().setComment("create table").build()
            );
        } catch (TableExistsException e) {
            // This is expected if we're continuing an existing report.
        }
    }

    /**
     * Updates the metadata associated with the report.
     */
    @Override
    public void updateReport(AllTablesReportDelta delta) {
        checkNotNull(delta, "delta");

        updateMetadata(delta);

        if (delta.getTable().isPresent()) {
            updateTableData(delta, delta.getTable().get());
        }
    }

    private void updateMetadata(AllTablesReportDelta delta) {
        boolean metadataUpdated = false;

        MapDeltaBuilder metadataBuilder = Deltas.mapBuilder();
        if (delta.getStartTime().isPresent()) {
            metadataBuilder.put("startTime", JsonHelper.formatTimestamp(delta.getStartTime().get()));
            metadataUpdated = true;
        }
        if (delta.getCompleteTime().isPresent()) {
            metadataBuilder.put("completeTime", JsonHelper.formatTimestamp(delta.getCompleteTime().get()));
            metadataUpdated = true;
        }
        if (delta.getSuccess().isPresent()) {
            metadataBuilder.put("success", delta.getSuccess().get());
            metadataUpdated = true;
        }
        if (delta.getPlacements().isPresent()) {
            metadataBuilder.update("placements", Deltas.mapBuilder().put(delta.getPlacements().get(), 1).build());
            metadataUpdated = true;
        }
        Delta metadataDelta = metadataBuilder.build();
        if (!metadataUpdated) {
            return;
        }

        _dataStore.update(
                getTableName(delta.getReportId()),
                REPORT_METADATA_KEY,
                TimeUUIDs.newUUID(),
                metadataDelta,
                new AuditBuilder().setComment(delta.getDescription()).build(),
                WriteConsistency.WEAK
        );
    }


    private void updateTableData(AllTablesReportDelta delta, AllTablesReportDelta.TableDelta tableDelta) {
        _dataStore.update(
                getTableName(delta.getReportId()),
                tableDelta.getTableName(),
                TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("tables", Deltas.mapBuilder()
                                .update(tableDelta.getTableId(),
                                        Deltas.mapBuilder()
                                        .putIfAbsent("placement", tableDelta.getPlacement())
                                        .putIfAbsent("facade", tableDelta.isFacade())
                                        .update("dropped", tableDelta.isDropped()
                                                ? Deltas.literal(true)
                                                : Deltas.conditional(Conditions.not(Conditions.equal(true)), Deltas.literal(false)))
                                        .update("shards", Deltas.mapBuilder()
                                                .put(String.valueOf(tableDelta.getShardId()),
                                                        JsonHelper.convert(tableDelta.getTableStatistics(), Map.class))
                                                .build())
                                        .build())
                                .build())
                        .build(),
                new AuditBuilder().setComment(delta.getDescription()).build(),
                WriteConsistency.WEAK);
    }

    /**
     * Returns the table data for a given report.  The caller can optionally return a partial report with ony
     * the requested tables.
     */
    @Override
    public TableReportMetadata getReportMetadata(String reportId) {
        checkNotNull(reportId, "reportId");

        final String reportTable = getTableName(reportId);

        Map<String, Object> metadata;
        try {
            metadata = _dataStore.get(reportTable, REPORT_METADATA_KEY);
        } catch (UnknownTableException e) {
            // The table is only unknown if the report doesn't exist
            throw new ReportNotFoundException(reportId);
        }

        if (Intrinsic.isDeleted(metadata)) {
            throw new ReportNotFoundException(reportId);
        }

        Date startTime = JsonHelper.parseTimestamp((String) metadata.get("startTime"));
        Date completeTime = null;
        if (metadata.containsKey("completeTime")) {
            completeTime = JsonHelper.parseTimestamp((String) metadata.get("completeTime"));
        }
        Boolean success = (Boolean) metadata.get("success");

        List<String> placements;
        Object placementMap = metadata.get("placements");
        if (placementMap != null) {
            placements = ImmutableList.copyOf(
                    JsonHelper.convert(placementMap, new TypeReference<Map<String, Object>>() {}).keySet());
        } else {
            placements = ImmutableList.of();
        }

        return new TableReportMetadata(reportId, startTime, completeTime, success, placements);
    }

    /**
     * Returns the matching table report entries for a report ID and query parameters.
     */
    @Override
    public Iterable<TableReportEntry> getReportEntries(String reportId, final AllTablesReportQuery query) {
        checkNotNull(reportId, "reportId");

        final String reportTable = getTableName(reportId);

        // Set up several filters based on the query attributes

        final Predicate<String> placementFilter = query.getPlacements().isEmpty()
                ? Predicates.<String>alwaysTrue()
                : Predicates.in(query.getPlacements());

        final Predicate<Boolean> droppedFilter = query.isIncludeDropped()
                ? Predicates.<Boolean>alwaysTrue()
                : Predicates.equalTo(false);

        final Predicate<Boolean> facadeFilter = query.isIncludeFacades()
                ? Predicates.<Boolean>alwaysTrue()
                : Predicates.equalTo(false);

        return new Iterable<TableReportEntry>() {
            @Override
            public Iterator<TableReportEntry> iterator() {
                return Iterators.limit(
                        Iterators.filter(
                                Iterators.transform(
                                        queryDataStoreForTableReportResults(reportTable, query),
                                        new Function<Map<String, Object>, TableReportEntry>() {
                                            @Nullable
                                            @Override
                                            public TableReportEntry apply(Map<String, Object> map) {
                                                if (Intrinsic.getId(map).startsWith("~")) {
                                                    // Skip this row, it's the metadata
                                                    return null;
                                                }
                                                return convertToTableReportEntry(
                                                        map, placementFilter, droppedFilter, facadeFilter);
                                            }
                                        }),
                                Predicates.notNull()
                        ),
                        query.getLimit()
                );
            }
        };
    }

    /**
     * Returns an iterator of report results based on the query configuration.  The results are guaranteed to be a
     * superset of results matching the original query; further filtering may be required to exactly match the
     * query.
     */
    private Iterator<Map<String, Object>> queryDataStoreForTableReportResults(final String reportTable, final AllTablesReportQuery query) {
        if (!query.getTableNames().isEmpty()) {
            // Querying for a specific set of tables.
            return Iterators.concat(
                    Iterators.transform(
                            query.getTableNames().iterator(),
                            new Function<String, Map<String, Object>>() {
                                @Override
                                public Map<String, Object> apply(String tableName) {
                                    return _dataStore.get(reportTable, tableName, ReadConsistency.STRONG);
                                }
                            }
                    ));
        }

        // Querying over the full set of tables

        return new AbstractIterator<Map<String, Object>>() {
            private String _from = query.getFromTable();
            private Iterator<Map<String, Object>> _batch = Iterators.emptyIterator();
            private long _limit = 0;

            @Override
            protected Map<String, Object> computeNext() {
                if (!_batch.hasNext()) {
                    if (_limit == 0) {
                        // First iteration get the requested number of results
                        _limit = query.getLimit();
                    } else {
                        // Subsequent requests iterate over all remaining results; dataStore will batch the results
                        _limit = Long.MAX_VALUE;
                    }

                    _batch = _dataStore.scan(reportTable, _from, _limit, false, ReadConsistency.STRONG);
                    if (!_batch.hasNext()) {
                        return endOfData();
                    }
                }

                Map<String, Object> result = _batch.next();
                _from = Intrinsic.getId(result);
                return result;
            }
        };
    }

    /**
     * Accepts a row from the table report and returns it converted into a TableReportEntry.  If the row is deleted
     * or if it doesn't match all of the configured filters then null is returned.
     */
    @Nullable
    private TableReportEntry convertToTableReportEntry(Map<String, Object> map, Predicate<String> placementFilter,
                                                       Predicate<Boolean> droppedFilter, Predicate<Boolean> facadeFilter) {
        if (Intrinsic.isDeleted(map)) {
            return null;
        }

        final String tableName = Intrinsic.getId(map);

        List<TableReportEntryTable> tables = Lists.newArrayListWithExpectedSize(map.size());
        for (Map.Entry<String, Object> entry : toMap(map.get("tables")).entrySet()) {
            TableReportEntryTable entryTable =
                    convertToTableReportEntryTable(entry.getKey(), toMap(entry.getValue()),
                            placementFilter, droppedFilter, facadeFilter);

            if (entryTable != null) {
                tables.add(entryTable);
            }
        }

        // If all tables were filtered then return null
        if (tables.isEmpty()) {
            return null;
        }

        return new TableReportEntry(tableName, tables);
    }

    /**
     * Accepts the table portion of a table report entry and converts it to a TableReportEntryTable.  If the
     * entry doesn't match all of the configured filters then null is returned.
     */
    @Nullable
    private TableReportEntryTable convertToTableReportEntryTable(
            String tableId,  Map<String, Object> map, Predicate<String> placementFilter,
            Predicate<Boolean> droppedFilter, Predicate<Boolean> facadeFilter) {

        // Check the filters for placement, dropped, and facade

        String placement = (String) map.get("placement");
        if (!placementFilter.apply(placement)) {
            return null;
        }

        Boolean dropped = Objects.firstNonNull((Boolean) map.get("dropped"), false);
        if (!droppedFilter.apply(dropped)) {
            return null;
        }

        Boolean facade = Objects.firstNonNull((Boolean) map.get("facade"), false);
        if (!facadeFilter.apply(facade)) {
            return null;
        }

        List<Integer> shards = Lists.newArrayList();

        // Aggregate the column, size and update statistics across all shards.

        TableStatistics.Aggregator aggregator = TableStatistics.newAggregator();

        Object shardJson = map.get("shards");

        if (shardJson != null) {
            Map<String, TableStatistics> shardMap = JsonHelper.convert(
                    shardJson, new TypeReference<Map<String, TableStatistics>>() {
            });

            for (Map.Entry<String, TableStatistics> entry : shardMap.entrySet()) {
                Integer shardId = Integer.parseInt(entry.getKey());
                shards.add(shardId);
                aggregator.add(entry.getValue());
            }
        }

        TableStatistics tableStatistics = aggregator.aggregate();
        Collections.sort(shards);

        return new TableReportEntryTable(tableId, placement, shards, dropped, facade, tableStatistics.getRecordCount(),
                tableStatistics.getColumnStatistics().toStatistics(),
                tableStatistics.getSizeStatistics().toStatistics(),
                tableStatistics.getUpdateTimeStatistics().toStatistics());
    }

    private String getTableName(String reportId) {
        return REPORT_TABLE_PREFIX + reportId;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> toMap(Object object) {
        return (Map<String, Object>) object;
    }
}
