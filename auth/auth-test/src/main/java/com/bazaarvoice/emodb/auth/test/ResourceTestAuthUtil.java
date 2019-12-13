package com.bazaarvoice.emodb.auth.test;

import com.bazaarvoice.emodb.auth.jersey.JerseyAuthConfiguration;
import com.bazaarvoice.emodb.auth.jersey.JerseyAuthConfigurationBuilder;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import io.dropwizard.testing.junit.ResourceTestRule;
import javax.ws.rs.container.DynamicFeature;
import org.apache.shiro.mgt.SecurityManager;

abstract public class ResourceTestAuthUtil {

    private ResourceTestAuthUtil() {
        // empty
    }

    /**
     * Call this method to set up authentication in the test's Jersey client.
     */
    public static void setUpResources(ResourceTestRule.Builder test, SecurityManager securityManager) {
        JerseyAuthConfiguration config = JerseyAuthConfigurationBuilder.build(securityManager);

        for (Object provider : config.getProviderInstances()) {
            test.addProvider(provider);
        }
        for (Class<?> providerClass : config.getProviderClasses()) {
            test.addProvider(providerClass);
        }
        for(DynamicFeature dynamicFeature : config.getDynamicFeatures()) {
            test.addProvider(dynamicFeature);
        }
    }
}
