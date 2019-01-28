package com.bazaarvoice.emodb.cachemgr;

import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.invalidate.DefaultInvalidationProvider;
import com.bazaarvoice.emodb.cachemgr.invalidate.DropwizardInvalidationClient;
import com.bazaarvoice.emodb.cachemgr.invalidate.InvalidationService;
import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfAdminHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.ostrich.ServiceRegistry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import javax.ws.rs.client.Client;
import org.apache.curator.framework.CuratorFramework;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class CacheManagerModuleTest {

    @Test
    public void testCacheManagerModule() {
        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                binder().requireExplicitBindings();

                // construct the minimum necessary elements to allow a CacheManager module to be created.
                bind(Client.class).toInstance(mock(Client.class));
                bind(DataCenters.class).toInstance(mock(DataCenters.class));
                bind(LifeCycleRegistry.class).toInstance(new SimpleLifeCycleRegistry());
                bind(ServiceRegistry.class).toInstance(mock(ServiceRegistry.class));
                bind(TaskRegistry.class).toInstance(mock(TaskRegistry.class));
                bind(CuratorFramework.class).annotatedWith(Global.class).toInstance(mock(CuratorFramework.class));
                bind(HostAndPort.class).annotatedWith(SelfHostAndPort.class).toInstance(HostAndPort.fromString("localhost:8080"));
                bind(HostAndPort.class).annotatedWith(SelfAdminHostAndPort.class).toInstance(HostAndPort.fromString("localhost:8081"));
                bind(String.class).annotatedWith(InvalidationService.class).toInstance("emodb-cachemgr");
                bind(MetricRegistry.class).asEagerSingleton();

                install(new CacheManagerModule());
            }
        });

        CacheRegistry cacheRegistry = injector.getInstance(CacheRegistry.class);

        assertNotNull(cacheRegistry);

        // Verify that some things we expect to be private are, indeed, private
        assertPrivate(injector, DropwizardInvalidationClient.class);
        assertPrivate(injector, DefaultInvalidationProvider.class);
    }

    private void assertPrivate(Injector injector, Class<?> type) {
        try {
            injector.getInstance(type);
            fail();
        } catch (ConfigurationException e) {
            // Expected
        }
    }
}
