package com.bazaarvoice.emodb.web.report.db;

import com.bazaarvoice.emodb.sor.api.report.TableReportEntry;
import com.bazaarvoice.emodb.sor.api.report.TableReportMetadata;
import com.bazaarvoice.emodb.web.report.AllTablesReportQuery;
import com.bazaarvoice.emodb.web.report.ReportLoader;
import com.google.inject.Inject;

/**
 * Implementation of {@link ReportLoader} that loads its results from an AllTablesReportDAO.
 */
public class DAOReportLoader implements ReportLoader {

    private final AllTablesReportDAO _allTablesReportDAO;

    @Inject
    public DAOReportLoader(AllTablesReportDAO allTablesReportDAO) {
        _allTablesReportDAO = allTablesReportDAO;
    }

    @Override
    public TableReportMetadata getTableReportMetadata(String reportId) {
        return _allTablesReportDAO.getReportMetadata(reportId);
    }

    @Override
    public Iterable<TableReportEntry> getTableReportEntries(String reportId, AllTablesReportQuery options) {
        return _allTablesReportDAO.getReportEntries(reportId, options);
    }
}
