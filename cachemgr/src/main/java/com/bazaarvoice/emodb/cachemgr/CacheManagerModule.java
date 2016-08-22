package com.bazaarvoice.emodb.cachemgr;

import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.core.DefaultCacheRegistry;
import com.bazaarvoice.emodb.cachemgr.invalidate.DefaultInvalidationProvider;
import com.bazaarvoice.emodb.cachemgr.invalidate.DropwizardInvalidationClient;
import com.bazaarvoice.emodb.cachemgr.invalidate.DropwizardInvalidationTask;
import com.bazaarvoice.emodb.cachemgr.invalidate.EndPointProvider;
import com.bazaarvoice.emodb.cachemgr.invalidate.ForeignDataCenterEndPointProvider;
import com.bazaarvoice.emodb.cachemgr.invalidate.ForeignDataCenters;
import com.bazaarvoice.emodb.cachemgr.invalidate.InvalidationService;
import com.bazaarvoice.emodb.cachemgr.invalidate.InvalidationServiceEndPointAdapter;
import com.bazaarvoice.emodb.cachemgr.invalidate.LocalDataCenter;
import com.bazaarvoice.emodb.cachemgr.invalidate.LocalDataCenterEndPointProvider;
import com.bazaarvoice.emodb.cachemgr.invalidate.RemoteInvalidationClient;
import com.bazaarvoice.emodb.cachemgr.invalidate.RemoteInvalidationListener;
import com.bazaarvoice.emodb.cachemgr.invalidate.RemoteInvalidationProvider;
import com.bazaarvoice.emodb.common.dropwizard.discovery.ManagedRegistration;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfAdminHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceRegistry;
import com.google.common.net.HostAndPort;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.sun.jersey.api.client.Client;

/**
 * Guice module for constructing a {@link CacheRegistry}.
 * <p>
 * Requires the following external references:
 * <ul>
 * <li> {@link DataCenters}
 * <li> {@link InvalidationService} string
 * <li> {@link LifeCycleRegistry}
 * <li> {@link TaskRegistry}
 * <li> @{@link SelfHostAndPort} {@link HostAndPort}
 * <li> @{@link SelfAdminHostAndPort} {@link HostAndPort}
 * <li> Jersey {@link Client}
 * <li> Bazaarvoice SOA {@link ServiceRegistry}
 * </ul>
 * Exports the following:
 * <ul>
 * <li> {@link CacheRegistry}
 * <li> {@link ManagedRegistration} annotated with {@link InvalidationService}
 * </ul>
 */
public class CacheManagerModule extends PrivateModule {

    @Override
    protected void configure() {
        // Bind the classes that implement intra- and cross-data center cache validation.
        bind(InvalidationServiceEndPointAdapter.class).asEagerSingleton();
        bind(EndPointProvider.class).annotatedWith(LocalDataCenter.class).to(LocalDataCenterEndPointProvider.class).asEagerSingleton();
        bind(EndPointProvider.class).annotatedWith(ForeignDataCenters.class).to(ForeignDataCenterEndPointProvider.class).asEagerSingleton();
        bind(RemoteInvalidationClient.class).to(DropwizardInvalidationClient.class).asEagerSingleton();
        bind(RemoteInvalidationProvider.class).to(DefaultInvalidationProvider.class).asEagerSingleton();
        bind(RemoteInvalidationListener.class).asEagerSingleton();
        bind(DropwizardInvalidationTask.class).asEagerSingleton();
        bind(DropwizardInvalidationClient.class).asEagerSingleton();

        // Bind the CacheRegistry instance that the rest of the application will consume.
        bind(CacheRegistry.class).to(DefaultCacheRegistry.class).asEagerSingleton();
        expose(CacheRegistry.class);

        // Expose the Ostrich cache manager endpoint so EmoService can start it once everything else is initialized.
        expose(ManagedRegistration.class).annotatedWith(InvalidationService.class);
    }

    @Provides @Singleton
    ServiceEndPoint provideLocalServiceEndPoint(InvalidationServiceEndPointAdapter endPointAdapter) {
        return endPointAdapter.toSelfEndPoint();
    }

    /** Integrates {@link CacheRegistry} life cycle events with Dropwizard. */
    @Provides @Singleton @InvalidationService
    ManagedRegistration provideOstrichRegistration(ServiceRegistry serviceRegistry, ServiceEndPoint endPoint) {
        return new ManagedRegistration(serviceRegistry, endPoint);
    }
}
