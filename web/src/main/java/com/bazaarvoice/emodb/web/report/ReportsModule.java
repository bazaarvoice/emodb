package com.bazaarvoice.emodb.web.report;

import com.bazaarvoice.emodb.common.dropwizard.guice.SystemTablePlacement;
import com.bazaarvoice.emodb.web.report.db.AllTablesReportDAO;
import com.bazaarvoice.emodb.web.report.db.DAOReportLoader;
import com.bazaarvoice.emodb.web.report.db.EmoTableAllTablesReportDAO;
import com.google.inject.Key;
import com.google.inject.PrivateModule;

public class ReportsModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(AllTablesReportDAO.class).to(EmoTableAllTablesReportDAO.class).asEagerSingleton();
        bind(ReportLoader.class).to(DAOReportLoader.class).asEagerSingleton();
        bind(AllTablesReportGenerator.class).asEagerSingleton();

        requireBinding(Key.get(String.class, SystemTablePlacement.class));

        expose(AllTablesReportGenerator.class);
        expose(ReportLoader.class);
    }
}
