package com.bazaarvoice.emodb.auth.test;

import com.bazaarvoice.emodb.auth.jersey.JerseyAuthConfiguration;
import com.bazaarvoice.emodb.auth.jersey.JerseyAuthConfigurationBuilder;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.sun.jersey.api.core.PackagesResourceConfig;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.shiro.mgt.SecurityManager;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

abstract public class ResourceTestAuthUtil {

    private ResourceTestAuthUtil() {
        // empty
    }

    /**
     * Call this method to set up authentication in the test's Jersey client.
     */
    public static void setUpResources(ResourceTestRule.Builder test, SecurityManager securityManager) {
        JerseyAuthConfiguration config = JerseyAuthConfigurationBuilder.build(securityManager);

        addResourceFilterFactories(test, config);
        for (Object provider : config.getProviderInstances()) {
            test.addProvider(provider);
        }
        for (Class<?> providerClass : config.getProviderClasses()) {
            test.addProvider(providerClass);
        }
    }

    @SuppressWarnings("unchecked")
    private static void addResourceFilterFactories(ResourceTestRule.Builder test, JerseyAuthConfiguration config) {
        // The ability to add properties is normally protected so we need to break in to update the resource filter
        // factories.
        Map<String, Object> properties;
        try {
            Field field = ResourceTestRule.Builder.class.getDeclaredField("properties");
            field.setAccessible(true);
            properties = (Map<String, Object>) field.get(test);
        } catch (Exception e) {
            Throwables.propagateIfPossible(e);
            throw new RuntimeException(e);
        }

        List resourceFilterFactories;

        // Add the new resource filter factories to any existing values
        Object existing = properties.get(PackagesResourceConfig.PROPERTY_RESOURCE_FILTER_FACTORIES);
        if (existing == null) {
            resourceFilterFactories = config.getResourceFilterFactories();
        } else {
            resourceFilterFactories = Lists.newArrayList();
            if (existing.getClass().isArray()) {
                resourceFilterFactories.addAll(Arrays.asList((Object[]) existing));
            } else if (Iterable.class.isAssignableFrom(existing.getClass())) {
                Iterables.addAll(resourceFilterFactories, (Iterable) existing);
            } else {
                resourceFilterFactories.add(existing);
            }
            resourceFilterFactories.addAll(config.getResourceFilterFactories());
        }

        properties.put(PackagesResourceConfig.PROPERTY_RESOURCE_FILTER_FACTORIES, resourceFilterFactories);
    }
}
