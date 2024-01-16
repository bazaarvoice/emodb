package com.bazaarvoice.emodb.auth.test;

import com.bazaarvoice.emodb.auth.jersey.JerseyAuthConfiguration;
import com.bazaarvoice.emodb.auth.jersey.JerseyAuthConfigurationBuilder;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.shiro.mgt.SecurityManager;
import javax.ws.rs.container.DynamicFeature;

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
