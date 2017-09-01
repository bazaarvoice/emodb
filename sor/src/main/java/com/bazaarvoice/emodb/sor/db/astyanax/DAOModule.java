package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.CqlDriverConfiguration;
import com.bazaarvoice.emodb.sor.DataStoreConfiguration;
import com.bazaarvoice.emodb.sor.core.DoubleWrite;
import com.bazaarvoice.emodb.sor.db.*;
import com.bazaarvoice.emodb.sor.db.cql.CqlReaderDAODelegate;
import com.bazaarvoice.emodb.sor.db.cql.CqlWriterDAODelegate;
import com.bazaarvoice.emodb.table.db.astyanax.DataCopyDAO;
import com.bazaarvoice.emodb.table.db.astyanax.DataPurgeDAO;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.codahale.metrics.MetricRegistry;
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

    private static final int DELTA_BLOCK_SIZE = 64 * 1024; // 64 KB block size (this must remain larger than (exclusive) 32 KB
    private static final int DELTA_PREFIX_LENGTH = 4;

    @Override
    protected void configure() {
        bind(Integer.class).annotatedWith(BlockSize.class).toInstance(DELTA_BLOCK_SIZE);
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
    @DoubleWrite
    boolean provideDoubleWrite(DataStoreConfiguration configuration) {
        return configuration.getMigrationPhase() == 1 || configuration.getMigrationPhase() == 2;
    }

    @Provides
    @Singleton
    DataReaderDAO provideDataReaderDAO(DataStoreConfiguration configuration, @CqlReaderDAODelegate DataReaderDAO delegate,
                                       PlacementCache placementCache, CqlDriverConfiguration driverConfig,
                                       ChangeEncoder changeEncoder, MetricRegistry metricRegistry,
                                       DAOUtils daoUtils, @PrefixLength int prefixLength) {
        if (configuration.getMigrationPhase() == 0 || configuration.getMigrationPhase() == 1) {
            return new CqlDataReaderDAO(delegate, placementCache, driverConfig, changeEncoder, metricRegistry);
        }
        else if (configuration.getMigrationPhase() == 2) {
            return new CqlBlockedDataReaderDAO(delegate, placementCache, driverConfig, changeEncoder, metricRegistry, daoUtils, prefixLength);
        }
        throw new RuntimeException("Invalid Migration Phase");
    }

    @Provides
    @Singleton
    @CqlReaderDAODelegate
    DataReaderDAO provideCqlReaderDAODelegate(DataStoreConfiguration configuration, PlacementCache placementCache,
                                              ChangeEncoder changeEncoder, MetricRegistry metricRegistry, DAOUtils daoUtils,
                                              @PrefixLength int prefixLength) {
        if (configuration.getMigrationPhase() == 0 || configuration.getMigrationPhase() == 1) {
            return new AstyanaxDataReaderDAO(placementCache, changeEncoder, metricRegistry);
        }
        else if (configuration.getMigrationPhase() == 2) {
            return new AstyanaxBlockedDataReaderDAO(placementCache, changeEncoder, metricRegistry, daoUtils, prefixLength);
        }
        throw new RuntimeException("Invalid Migration Phase");
    }

    @Provides
    @Singleton
    DataCopyDAO provideDataCopyDAO(DataStoreConfiguration configuration, PlacementCache placementCache,
                                   ChangeEncoder changeEncoder, MetricRegistry metricRegistry, DAOUtils daoUtils,
                                   @PrefixLength int prefixLength) {
        if (configuration.getMigrationPhase() == 0 || configuration.getMigrationPhase() == 1) {
            return new AstyanaxDataReaderDAO(placementCache, changeEncoder, metricRegistry);
        }
        else if (configuration.getMigrationPhase() == 2) {
            return new AstyanaxBlockedDataReaderDAO(placementCache, changeEncoder, metricRegistry, daoUtils, prefixLength);
        }
        throw new RuntimeException("Invalid Migration Phase");

    }

    @Provides
    @Singleton
    AstyanaxKeyScanner provideAstyanaxKeyScanner(DataStoreConfiguration configuration, PlacementCache placementCache,
                                                       ChangeEncoder changeEncoder, MetricRegistry metricRegistry, DAOUtils daoUtils,
                                                       @PrefixLength int prefixLength) {
        if (configuration.getMigrationPhase() == 0 || configuration.getMigrationPhase() == 1) {
            return new AstyanaxDataReaderDAO(placementCache, changeEncoder, metricRegistry);
        }
        else if (configuration.getMigrationPhase() == 2) {
            return new AstyanaxBlockedDataReaderDAO(placementCache, changeEncoder, metricRegistry, daoUtils, prefixLength);
        }
        throw new RuntimeException("Invalid Migration Phase");
    }




}