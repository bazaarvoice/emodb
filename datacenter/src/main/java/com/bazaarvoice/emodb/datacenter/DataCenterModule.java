package com.bazaarvoice.emodb.datacenter;

import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.datacenter.core.DataCenterAnnouncer;
import com.bazaarvoice.emodb.datacenter.core.DefaultDataCenters;
import com.bazaarvoice.emodb.datacenter.core.IgnoredDataCenters;
import com.bazaarvoice.emodb.datacenter.core.SelfCassandraDataCenter;
import com.bazaarvoice.emodb.datacenter.core.SelfDataCenter;
import com.bazaarvoice.emodb.datacenter.core.SelfDataCenterAdmin;
import com.bazaarvoice.emodb.datacenter.core.SystemDataCenter;
import com.bazaarvoice.emodb.datacenter.db.DataCenterDAO;
import com.bazaarvoice.emodb.datacenter.db.emo.EmoDataCenterDAO;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import java.net.URI;
import java.util.Set;

/**
 * Guice module for constructing an instance of {@link DataCenters}.
 * <p>
 * Requires the following external references:
 * <ul>
 * <li> {@link DataCenterConfiguration}
 * <li> {@link DataStore}
 * <li> {@link LifeCycleRegistry}
 * <li> {@link ServerCluster}</li>
 * </ul>
 * Exports the following:
 * <ul>
 * <li> {@link DataCenters}
 * </ul>
 */
public class DataCenterModule extends PrivateModule {
    private final EmoServiceMode _serviceMode;

    public DataCenterModule(EmoServiceMode serviceMode) {
        _serviceMode = serviceMode;
    }

    @Override
    protected void configure() {
        bind(DataCenterDAO.class).to(EmoDataCenterDAO.class).asEagerSingleton();

        // If this is a server that needs to coordinate with servers in other data centers, announce this data center.
        if (_serviceMode.specifies(EmoServiceMode.Aspect.dataCenter_announce)) {
            bind(DataCenterAnnouncer.class).asEagerSingleton();
        }

        bind(DataCenters.class).to(DefaultDataCenters.class).asEagerSingleton();
        expose(DataCenters.class);
    }

    @Provides @Singleton @SelfDataCenter
    String provideCurrentDataCenter(DataCenterConfiguration configuration) {
        return configuration.getCurrentDataCenter();
    }

    @Provides @Singleton @SelfCassandraDataCenter
    String provideCassandraDataCenter(DataCenterConfiguration configuration) {
        return configuration.getCassandraDataCenter();
    }

    @Provides @Singleton @SelfDataCenter
    URI provideDataCenterServiceUri(DataCenterConfiguration configuration) {
        return configuration.getDataCenterServiceUri();
    }

    @Provides @Singleton @SelfDataCenterAdmin
    URI provideDataCenterAdminUri(DataCenterConfiguration configuration) {
        return configuration.getDataCenterAdminUri();
    }

    @Provides @Singleton @SystemDataCenter
    String provideSystemDataCenter(DataCenterConfiguration configuration) {
        return configuration.getSystemDataCenter();
    }

    @Provides @Singleton @IgnoredDataCenters
    Set<String> provideIgnoredDataCenters(DataCenterConfiguration configuration) {
        return configuration.getIgnoredDataCenters();
    }

}
