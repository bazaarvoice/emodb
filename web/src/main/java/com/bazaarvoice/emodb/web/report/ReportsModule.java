package com.bazaarvoice.emodb.web.report;

import com.bazaarvoice.emodb.sor.DataStoreConfiguration;
import com.bazaarvoice.emodb.table.db.astyanax.SystemTablePlacement;
import com.bazaarvoice.emodb.web.report.db.AllTablesReportDAO;
import com.bazaarvoice.emodb.web.report.db.DAOReportLoader;
import com.bazaarvoice.emodb.web.report.db.EmoTableAllTablesReportDAO;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class ReportsModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(AllTablesReportDAO.class).to(EmoTableAllTablesReportDAO.class).asEagerSingleton();
        bind(ReportLoader.class).to(DAOReportLoader.class).asEagerSingleton();
        bind(AllTablesReportGenerator.class).asEagerSingleton();

        expose(AllTablesReportGenerator.class);
        expose(ReportLoader.class);
    }

    @Provides @Singleton @SystemTablePlacement
    String provideSystemTablePlacement(DataStoreConfiguration configuration) {
        return configuration.getSystemTablePlacement();
    }
}
