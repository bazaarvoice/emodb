package com.bazaarvoice.emodb.web.settings;

import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.sor.DataStoreConfiguration;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.common.dropwizard.guice.SystemTablePlacement;
import com.google.inject.*;

/**
 * Guice module which configures globally accessible server settings.
 *  * <p>
 * Requires the following external references:
 * <ul>
 * <li> {@link DataStore}
 * <li> {@link DataStoreConfiguration}
 * <li> {@link CacheRegistry}
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

        requireBinding(Key.get(String.class, SystemTablePlacement.class));

        expose(SettingsRegistry.class);
        expose(Settings.class);
    }

    @Provides @Singleton @SettingsCacheRegistry
    CacheRegistry provideSettingsCacheRegistry(CacheRegistry cacheRegistry) {
        return cacheRegistry.withNamespace("settings");
    }

    @Provides @Singleton
    SettingsManager provideSettings(@SettingsCacheRegistry CacheRegistry cacheRegistry,
                                    Provider<DataStore> dataStore, @SystemTablePlacement String placement) {
        // Note:  To prevent potential circular dependencies while constructing SettingsManager a Provider for the
        //        DataStore must be injected, deferring resolution of the DataStore until after all related
        //        objects are constructed.
        return new SettingsManager(dataStore, SETTINGS_TABLE, placement, cacheRegistry);
    }
}
