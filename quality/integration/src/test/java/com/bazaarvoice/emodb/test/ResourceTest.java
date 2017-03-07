package com.bazaarvoice.emodb.test;

import com.bazaarvoice.emodb.auth.SecurityManagerBuilder;
import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.test.ResourceTestAuthUtil;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.jersey.ExceptionMappers;
import com.bazaarvoice.emodb.web.throttling.ConcurrentRequestsThrottlingFilter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import com.sun.jersey.spi.container.ResourceFilterFactory;
import io.dropwizard.testing.junit.ResourceTestRule;
import test.integration.databus.DatabusJerseyTest;

import javax.servlet.http.HttpServletRequest;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

import static org.mockito.Mockito.mock;

public abstract class ResourceTest {

    private ConcurrentRequestsThrottlingFilter _concurrentRequestsThrottlingFilter;

    protected static ResourceTestRule setupResourceTestRule(List<Object> resourceList, ApiKey apiKey, ApiKey unauthorizedKey, String typeName) {
        return setupResourceTestRule(resourceList, ImmutableList.of(), apiKey, unauthorizedKey, typeName);
    }

    protected static ResourceTestRule setupResourceTestRule(List<Object> resourceList, List<Object> filters, ApiKey apiKey, ApiKey unauthorizedKey, String typeName) {
        InMemoryAuthIdentityManager<ApiKey> authIdentityManager = new InMemoryAuthIdentityManager<>(ApiKey.class);
        authIdentityManager.updateIdentity(apiKey);
        authIdentityManager.updateIdentity(unauthorizedKey);

        EmoPermissionResolver permissionResolver = new EmoPermissionResolver(mock(DataStore.class), mock(BlobStore.class));
        InMemoryPermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        permissionManager.updateForRole(
                typeName + "-role", new PermissionUpdateRequest().permit(typeName + "|*|*"));

        return setupResourceTestRule(resourceList, filters, authIdentityManager, permissionManager);
    }

    protected static ResourceTestRule setupResourceTestRule(List<Object> resourceList, AuthIdentityManager<ApiKey> authIdentityManager, PermissionManager permissionManager) {
        return setupResourceTestRule(resourceList, ImmutableList.of(), authIdentityManager, permissionManager);
    }

    protected static ResourceTestRule setupResourceTestRule(List<Object> resourceList, List<Object> filters,
                                                            AuthIdentityManager<ApiKey> authIdentityManager,
                                                            PermissionManager permissionManager) {
        ResourceTestRule.Builder resourceTestRuleBuilder = ResourceTestRule.builder();

        for (Object resource : resourceList) {
            resourceTestRuleBuilder.addResource(resource);
        }

        List<ResourceFilterFactory> resourceFilterFactories = Lists.newArrayList();
        List<ContainerRequestFilter> containerRequestFilters = Lists.newArrayList();
        List<ContainerResponseFilter> containerResponseFilters = Lists.newArrayList();

        for (Object filter : filters) {
            if (filter instanceof ResourceFilterFactory) {
                resourceFilterFactories.add((ResourceFilterFactory) filter);
            }
            if (filter instanceof ContainerRequestFilter) {
                containerRequestFilters.add((ContainerRequestFilter) filter);
            }
            if (filter instanceof ContainerResponseFilter) {
                containerResponseFilters.add((ContainerResponseFilter) filter);
            }
        }

        resourceTestRuleBuilder.addProperty(ResourceConfig.PROPERTY_RESOURCE_FILTER_FACTORIES, resourceFilterFactories);
        resourceTestRuleBuilder.addProperty(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS, containerRequestFilters);
        resourceTestRuleBuilder.addProperty(ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS, containerResponseFilters);

        // Jersey tests don't inject Context parameters, so create an injector to provide a mock instance.
        resourceTestRuleBuilder.addProvider(new DatabusJerseyTest.ContextInjectableProvider<>(HttpServletRequest.class, mock(HttpServletRequest.class)));

        ResourceTestAuthUtil.setUpResources(resourceTestRuleBuilder, SecurityManagerBuilder.create()
                .withAuthIdentityManager(authIdentityManager)
                .withPermissionManager(permissionManager)
                .build());

        for (Object mapper : ExceptionMappers.getMappers()) {
            resourceTestRuleBuilder.addProvider(mapper);
        }
        for (Class mapperType : ExceptionMappers.getMapperTypes()) {
            resourceTestRuleBuilder.addProvider(mapperType);
        }

        ResourceTestRule resourceTestRule = resourceTestRuleBuilder.build();

        // Write Date objects using ISO8601 strings instead of numeric milliseconds-since-1970.
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        fmt.setTimeZone(TimeZone.getTimeZone("GMT"));
        resourceTestRule.getObjectMapper().setDateFormat(fmt);

        return resourceTestRule;
    }
}
