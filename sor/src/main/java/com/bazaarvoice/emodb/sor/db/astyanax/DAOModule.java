package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.sor.DataStoreConfiguration;
import com.bazaarvoice.emodb.sor.DeltaMigrationPhase;
import com.bazaarvoice.emodb.sor.db.*;
import com.bazaarvoice.emodb.sor.db.cql.CqlReaderDAODelegate;
import com.bazaarvoice.emodb.sor.db.cql.CqlWriterDAODelegate;
import com.bazaarvoice.emodb.table.db.astyanax.DataCopyDAO;
import com.bazaarvoice.emodb.table.db.astyanax.DataPurgeDAO;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;

/**
 * Guice module for DAO implementations.  Separate from {@link com.bazaarvoice.emodb.sor.DataStoreModule} to allow
 * private bindings only used by the DAOs.  Required bindings are documented in DataStoreModule.
 *
 * @see com.bazaarvoice.emodb.sor.DataStoreModule
 */
public class DAOModule extends PrivateModule {

    private static final int DELTA_PREFIX_LENGTH = 4;

    @Override
    protected void configure() {
        bind(Integer.class).annotatedWith(PrefixLength.class).toInstance(DELTA_PREFIX_LENGTH);
        bind(DAOUtils.class).asEagerSingleton();
        bind(DataWriterDAO.class).annotatedWith(CqlWriterDAODelegate.class).to(AstyanaxDataWriterDAO.class).asEagerSingleton();
        bind(DataWriterDAO.class).annotatedWith(AstyanaxWriterDAODelegate.class).to(CqlDataWriterDAO.class).asEagerSingleton();
        bind(DataWriterDAO.class).to(CqlDataWriterDAO.class).asEagerSingleton();
        bind(DataPurgeDAO.class).to(AstyanaxDataWriterDAO.class).asEagerSingleton();
        bind(MigratorWriterDAO.class).to(CqlDataWriterDAO.class).asEagerSingleton();
        bind(MigratorReaderDAO.class).to(CqlDataReaderDAO.class).asEagerSingleton();

        // Explicit bindings so objects don't get created as a just-in-time binding in the root injector.
        // This needs to be done for just about anything that has only public dependencies.
        bind(AstyanaxDataWriterDAO.class).asEagerSingleton();
        bind(CqlDataWriterDAO.class).asEagerSingleton();

        // For migration stages, will be reverted in future version
        bind(AstyanaxDataReaderDAO.class).asEagerSingleton();
        bind(AstyanaxBlockedDataReaderDAO.class).asEagerSingleton();
        bind(CqlDataReaderDAO.class).asEagerSingleton();
        bind(CqlBlockedDataReaderDAO.class).asEagerSingleton();

        expose(DataReaderDAO.class);
        expose(DataWriterDAO.class);
        expose(DataCopyDAO.class);
        expose(DataPurgeDAO.class);
        expose(MigratorReaderDAO.class);
        expose(MigratorWriterDAO.class);
    }

    @Provides
    @Singleton
    ChangeEncoder provideChangeEncoder(DataStoreConfiguration configuration) {
        return new DefaultChangeEncoder(configuration.getDeltaEncodingVersion());
    }

    @Provides
    @WriteToLegacyDeltaTable
    boolean provideWriteToLegacyDeltaTable(DataStoreConfiguration configuration) {
        DeltaMigrationPhase migrationPhase = configuration.getMigrationPhase();
        return migrationPhase.isWriteToLegacyDeltaTables();
    }

    @Provides
    @WriteToBlockedDeltaTable
    boolean provideWriteToBlockedDeltaTable(DataStoreConfiguration configuration) {
        DeltaMigrationPhase migrationPhase = configuration.getMigrationPhase();
        return migrationPhase.isWriteToBlockedDeltaTables();
    }

    @Provides
    @Singleton
    DataReaderDAO provideDataReaderDAO(DataStoreConfiguration configuration, Provider<CqlDataReaderDAO> legacyReader,
                                       Provider<CqlBlockedDataReaderDAO> blockedReader) {

        return configuration.getMigrationPhase().isReadFromLegacyDeltaTables() ? legacyReader.get() : blockedReader.get();
    }

    @Provides
    @Singleton
    @CqlReaderDAODelegate
    DataReaderDAO provideCqlReaderDAODelegate(DataStoreConfiguration configuration,
                                              Provider<AstyanaxDataReaderDAO> legacyReader,
                                              Provider<AstyanaxBlockedDataReaderDAO> blockedReader) {
        return configuration.getMigrationPhase().isReadFromLegacyDeltaTables() ? legacyReader.get() : blockedReader.get();
    }

    @Provides
    @Singleton
    DataCopyDAO provideDataCopyDAO(DataStoreConfiguration configuration, Provider<AstyanaxDataReaderDAO> legacyReader,
                                   Provider<AstyanaxBlockedDataReaderDAO> blockedReader) {

        return configuration.getMigrationPhase().isReadFromLegacyDeltaTables() ? legacyReader.get() : blockedReader.get();
    }

    @Provides
    @Singleton
    AstyanaxKeyScanner provideAstyanaxKeyScanner(DataStoreConfiguration configuration, Provider<AstyanaxDataReaderDAO> legacyReader,
                                                 Provider<AstyanaxBlockedDataReaderDAO> blockedReader) {

        return configuration.getMigrationPhase().isReadFromLegacyDeltaTables() ? legacyReader.get() : blockedReader.get();
    }

    @Provides
    @Singleton
    @BlockSize
    int provideBlockSize(DataStoreConfiguration configuration) {
        return configuration.getDeltaBlockSizeInKb() * 1024;
    }





}