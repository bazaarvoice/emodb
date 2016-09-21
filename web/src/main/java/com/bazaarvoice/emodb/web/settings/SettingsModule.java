package com.bazaarvoice.emodb.web.settings;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkTimestampSerializer;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkValueStore;
import com.bazaarvoice.emodb.sor.DataStoreConfiguration;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.table.db.astyanax.SystemTablePlacement;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.curator.framework.CuratorFramework;

import java.time.Clock;

/**
 * Guice module which configures globally accessible server settings.
 *  * <p>
 * Requires the following external references:
 * <ul>
 * <li> {@link DataStore}
 * <li> {@link LifeCycleRegistry}
 * <li> {@link DataStoreConfiguration}
 * <li> @SettingsZooKeeper {@link CuratorFramework}
 * <li> {@link Clock}
 * </ul>
 * Exports the following:
 * <ul>
 * <li> {@link SettingsRegistry}
 * <li> {@link Settings}
 * </ul>
 */
public class SettingsModule extends PrivateModule {

    private final static String SETTINGS_TABLE = "__system:settings";

    @Override
    protected void configure() {
        bind(SettingsRegistry.class).to(SettingsManager.class);
        bind(Settings.class).to(SettingsManager.class);

        expose(SettingsRegistry.class);
        expose(Settings.class);
    }

    /**
     * Returns the binding for the system table placement.  This implementation piggy-backs on the system table
     * placement from the DataStore configuration.  This isn't optimal since it's violating separation of concerns,
     * but until there is a globally accessible system table placement available for injection this will
     * have to suffice.
     */
    @Provides @Singleton @SystemTablePlacement
    String provideSystemTablePlacement(DataStoreConfiguration config) {
        return config.getSystemTablePlacement();
    }

    @Provides @Singleton @Named("lastUpdatedStore")
    ValueStore<Long> provideLastUpdatedValueStore(@SettingsZooKeeper CuratorFramework curator,
                                                  LifeCycleRegistry lifeCycleRegistry) {
        return lifeCycleRegistry.manage(new ZkValueStore<>(curator, "settings", new ZkTimestampSerializer()));
    }

    @Provides @Singleton
    SettingsManager provideSettings(@Named("lastUpdatedStore") ValueStore<Long> lastUpdated,
                                    Provider<DataStore> dataStore, @SystemTablePlacement String placement,
                                    LifeCycleRegistry lifeCycleRegistry, Clock clock) {
        // Note:  To prevent potential circular dependencies while constructing SettingsManager a Provider for the
        //        DataStore must be injected, deferring resolution of the DataStore until after all related
        //        objects are constructed.
        return new SettingsManager(
                lastUpdated, dataStore, SETTINGS_TABLE, placement, lifeCycleRegistry, clock);
    }
}
