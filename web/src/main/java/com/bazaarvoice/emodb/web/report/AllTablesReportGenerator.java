package com.bazaarvoice.emodb.web.report;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.sor.db.MultiTableScanOptions;
import com.bazaarvoice.emodb.sor.db.MultiTableScanResult;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.db.RecordEntryRawMetadata;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.bazaarvoice.emodb.table.db.astyanax.TableUuidFormat;
import com.bazaarvoice.emodb.web.report.db.AllTablesReportDAO;
import com.bazaarvoice.emodb.web.report.db.AllTablesReportDelta;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class AllTablesReportGenerator {
    private final DataTools _dataTools;
    private final AllTablesReportDAO _allTablesReportDAO;

    @Inject
    public AllTablesReportGenerator(DataTools dataTools, AllTablesReportDAO allTablesReportDAO) {
        _dataTools = checkNotNull(dataTools, "dataTools");
        _allTablesReportDAO = checkNotNull(allTablesReportDAO, "allTablesReportDAO");
    }

    public AllTablesReportResult runReport(String reportId, AllTablesReportOptions options) {
        checkNotNull(reportId, "reportId");
        checkNotNull(options, "options");

        // Validate that the report table exists
        _allTablesReportDAO.verifyOrCreateReport(reportId);

        AllTablesReportResult report = new AllTablesReportResult()
                .withReportId(reportId)
                .withStartTime(new Date());

        boolean firstPlacement = true;

        try (TableSet tables = _dataTools.createTableSet()) {
            // Mark the report as being created
            _allTablesReportDAO.updateReport(new AllTablesReportDelta(reportId, "start report")
                    .withStartTime(report.getStartTime()));

            for (String placement : getPlacements(options)) {
                // Mark the placement is being evaluated.  Use unique map keys so that different placements can be
                // used on different runs of the same report without Collection interference in the deltas.
                _allTablesReportDAO.updateReport(new AllTablesReportDelta(reportId, "start placement")
                        .withPlacement(placement));

                MultiTableScanOptions scanOptions = new MultiTableScanOptions()
                        .setPlacement(placement)
                        .setIncludeDeletedTables(true)
                        .setIncludeMirrorTables(true);

                // For the first placement, if the options specify starting at a specific shard/table ID then set the scan options to match.
                if (firstPlacement) {
                    // TODO:  Need to resolve how to start at a shard and table since ranges are abstracted away as byte buffers
//                    scanOptions.setScanRange(ScanBoundary.from(
//                            options.getFromShardId().orNull(),
//                            options.getFromTableUuid().orNull()));
                    firstPlacement = false;
                }

                ReadConsistency consistency = options.isReadOnly() ? ReadConsistency.WEAK : ReadConsistency.STRONG;
                Iterator<MultiTableScanResult> results =
                        _dataTools.multiTableScan(scanOptions, tables, LimitCounter.max(), consistency, null);

                int lastShardId = -1;
                long lastTableUuid = -1;
                String tableName = null;
                boolean dropped = false;
                boolean facade = false;
                LongReportHistogram columnStatistics = new LongReportHistogram();
                LongReportHistogram sizeStatistics = new LongReportHistogram();
                DateReportHistogram updateTimeStatistics = new DateReportHistogram();

                // Loop through the results.  The results are ordered by [shard, table], so look for changes in these
                // values to determine end of table and/or shard.
                while (results.hasNext()) {
                    MultiTableScanResult result = results.next();

                    // Roll the table and shard if necessary
                    if (result.getTableUuid() != lastTableUuid) {
                        String tableId = TableUuidFormat.encode(lastTableUuid);
                        report.setLastCompleteTableUuid(tableId);
                        lastTableUuid = result.getTableUuid();

                        // Save the shard/table data
                        if (tableName != null) {
                            saveShardTable(reportId, tableId, tableName, placement, dropped, facade, lastShardId,
                                    columnStatistics, sizeStatistics, updateTimeStatistics);
                        }

                        // Reset the statistics
                        columnStatistics.clear();
                        sizeStatistics.clear();
                        updateTimeStatistics.clear();

                        tableName = result.getTable().getName();
                        dropped = result.isDropped();
                        facade = result.getTable().isFacade();
                    }

                    if (lastShardId != result.getShardId()) {
                        report.setLastCompleteShard(lastShardId);
                        report.setLastCompleteTableUuid(null);
                        lastShardId = result.getShardId();
                    }

                    // Update the metrics for this table based on the current record
                    Record record = result.getRecord();

                    long columns = 0;
                    long size = 0;

                    Iterator<RecordEntryRawMetadata> rawMetadataIter = record.rawMetadata();
                    while (rawMetadataIter.hasNext()) {
                        RecordEntryRawMetadata rawMetadata = rawMetadataIter.next();
                        columns += 1;
                        size += rawMetadata.getSize();

                        updateTimeStatistics.update(new Date(rawMetadata.getTimestamp()));
                    }

                    columnStatistics.update(columns);
                    sizeStatistics.update(size);
                }

                // Save the shard/table data for the final table (if present)
                if (tableName != null) {
                    saveShardTable(reportId, TableUuidFormat.encode(lastTableUuid), tableName, placement,
                            dropped, facade, lastShardId, columnStatistics, sizeStatistics, updateTimeStatistics);
                }

                report.setLastCompleteShard(-1);
                report.setLastCompleteTableUuid(null);

                report.setLastCompletePlacement(placement);
            }

            report.setSuccess(true);
        } catch (Throwable t) {
            report.setSuccess(false);
            throw Throwables.propagate(t);
        } finally {
            report.setCompleteTime(new Date());

            // Mark the report as complete
            _allTablesReportDAO.updateReport(new AllTablesReportDelta(reportId, "complete report")
                    .withCompleteTime(report.getCompleteTime())
                    .withSuccess(report.isSuccess()));
        }

        return report;
    }

    private void saveShardTable(String reportId, String tableId, String tableName, String placement,
                                boolean dropped, boolean facade, int shardId,
                                LongReportHistogram columnStatistics,
                                LongReportHistogram sizeStatistics,
                                DateReportHistogram updateTimeStatistics) {
        // Save the shard/table data
        TableStatistics tableStatistics = new TableStatistics((int) columnStatistics.count(),
                columnStatistics, sizeStatistics, updateTimeStatistics);

        _allTablesReportDAO.updateReport(new AllTablesReportDelta(reportId, "add table")
                .withTable(new AllTablesReportDelta.TableDelta(
                        tableId, tableName, placement, dropped, facade, shardId, tableStatistics)));
    }

    private List<String> getPlacements(AllTablesReportOptions options) {
        Collection<String> placements = _dataTools.getTablePlacements(true, true);
        if (options.getPlacements().isPresent()) {
            placements = Sets.intersection(options.getPlacements().get(), ImmutableSet.copyOf(placements));
        }
        // Sort the placements so they will always be analyzed in a deterministic order.
        return Ordering.natural().immutableSortedCopy(placements);
    }
}
