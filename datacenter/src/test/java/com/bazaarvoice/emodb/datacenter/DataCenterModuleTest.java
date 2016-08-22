package com.bazaarvoice.emodb.datacenter;

import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.datacenter.api.KeyspaceDiscovery;
import com.bazaarvoice.emodb.datacenter.db.emo.EmoDataCenterDAO;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.google.inject.AbstractModule;
import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import org.testng.annotations.Test;

import java.net.URI;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class DataCenterModuleTest {

    @Test
    public void testServerMode() {
        Injector injector = createInjector(EmoServiceMode.STANDARD_ALL);

        assertNotNull(injector.getInstance(DataCenters.class));

        // Verify that some things we expect to be private are, indeed, private
        assertPrivate(injector, EmoDataCenterDAO.class);
    }

    @Test
    public void testCliMode() {
        Injector injector = createInjector(EmoServiceMode.CLI_TOOL);

        assertNotNull(injector.getInstance(DataCenters.class));
    }

    private Injector createInjector(final EmoServiceMode serviceMode) {
        return Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                binder().requireExplicitBindings();

                // construct the minimum necessary elements to allow a DataCenter module to be created.
                bind(DataCenterConfiguration.class).toInstance(new DataCenterConfiguration()
                        .setCurrentDataCenter("us-east")
                        .setSystemDataCenter("us-east")
                        .setDataCenterServiceUri(URI.create("http://localhost:8080"))
                        .setDataCenterAdminUri(URI.create("http://localhost:8080")));

                bind(DataStore.class).toInstance(mock(DataStore.class));
                bind(KeyspaceDiscovery.class).annotatedWith(Names.named("blob")).toInstance(mock(KeyspaceDiscovery.class));
                bind(KeyspaceDiscovery.class).annotatedWith(Names.named("sor")).toInstance(mock(KeyspaceDiscovery.class));
                bind(LifeCycleRegistry.class).toInstance(new SimpleLifeCycleRegistry());
                bind(String.class).annotatedWith(ServerCluster.class).toInstance("local_default");

                install(new DataCenterModule(serviceMode));
            }
        });
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
