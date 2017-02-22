package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.sor.DataStoreConfiguration;
import com.bazaarvoice.emodb.sor.db.DataReaderDAO;
import com.bazaarvoice.emodb.sor.db.DataWriterDAO;
import com.bazaarvoice.emodb.sor.db.cql.CqlReaderDAODelegate;
import com.bazaarvoice.emodb.table.db.astyanax.DataCopyDAO;
import com.bazaarvoice.emodb.table.db.astyanax.DataPurgeDAO;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

/**
 * Guice module for DAO implementations.  Separate from {@link com.bazaarvoice.emodb.sor.DataStoreModule} to allow
 * private bindings only used by the DAOs.  Required bindings are documented in DataStoreModule.
 *
 * @see com.bazaarvoice.emodb.sor.DataStoreModule
 */
public class DAOModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(DataReaderDAO.class).annotatedWith(CqlReaderDAODelegate.class).to(AstyanaxDataReaderDAO.class).asEagerSingleton();
        bind(DataReaderDAO.class).to(CqlDataReaderDAO.class).asEagerSingleton();
        bind(DataWriterDAO.class).to(AstyanaxDataWriterDAO.class).asEagerSingleton();
        bind(DataCopyDAO.class).to(AstyanaxDataReaderDAO.class).asEagerSingleton();
        bind(DataPurgeDAO.class).to(AstyanaxDataWriterDAO.class).asEagerSingleton();

        // Explicit bindings so objects don't get created as a just-in-time binding in the root injector.
        // This needs to be done for just about anything that has only public dependencies.
        bind(AstyanaxDataReaderDAO.class).asEagerSingleton();

        expose(DataReaderDAO.class);
        expose(DataWriterDAO.class);
        expose(DataCopyDAO.class);
        expose(DataPurgeDAO.class);
    }

    @Provides
    @Singleton
    ChangeEncoder provideChangeEncoder(DataStoreConfiguration configuration) {
        return new DefaultChangeEncoder(configuration.getDeltaEncodingVersion());
    }
}
