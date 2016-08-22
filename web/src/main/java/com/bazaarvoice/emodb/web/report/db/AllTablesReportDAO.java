package com.bazaarvoice.emodb.web.report.db;

import com.bazaarvoice.emodb.sor.api.report.TableReportEntry;
import com.bazaarvoice.emodb.sor.api.report.TableReportMetadata;
import com.bazaarvoice.emodb.web.report.AllTablesReportQuery;

public interface AllTablesReportDAO {
    /**
     * Verifies a report exists, or creates one if it doesn't.
     */
    void verifyOrCreateReport(String reportId);

    /**
     * Adds information to an existing report.
     */
    void updateReport(AllTablesReportDelta delta);

    /**
     * Returns the metadata for a report.
     */
    TableReportMetadata getReportMetadata(String reportId);

    /**
     * Returns results for a report.
     */
    Iterable<TableReportEntry> getReportEntries(String reportId, AllTablesReportQuery query);
}
