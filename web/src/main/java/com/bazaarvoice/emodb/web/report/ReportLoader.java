package com.bazaarvoice.emodb.web.report;

import com.bazaarvoice.emodb.sor.api.report.TableReportEntry;
import com.bazaarvoice.emodb.sor.api.report.TableReportMetadata;

public interface ReportLoader {

    TableReportMetadata getTableReportMetadata(String reportId);

    Iterable<TableReportEntry> getTableReportEntries(String reportId, AllTablesReportQuery options);
}
