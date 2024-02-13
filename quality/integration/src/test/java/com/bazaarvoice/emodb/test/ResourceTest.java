package com.bazaarvoice.emodb.test;

import com.bazaarvoice.emodb.auth.SecurityManagerBuilder;
import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyModification;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.role.InMemoryRoleManager;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.auth.role.RoleManager;
import com.bazaarvoice.emodb.auth.role.RoleModification;
import com.bazaarvoice.emodb.auth.test.ResourceTestAuthUtil;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.jersey.ExceptionMappers;
import com.bazaarvoice.emodb.web.throttling.ConcurrentRequestsThrottlingFilter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import io.dropwizard.testing.junit.ResourceTestRule;
import test.integration.databus.DatabusJerseyTest;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class ResourceTest {

    private ConcurrentRequestsThrottlingFilter _concurrentRequestsThrottlingFilter;

    protected static ResourceTestRule setupResourceTestRule(List<Object> resourceList, Map<String, ApiKey> apiKeys,
                                                            Multimap<String, String> permissionsByRole) {
        return setupResourceTestRule(resourceList, ImmutableList.of(), apiKeys, permissionsByRole);
    }

    protected static ResourceTestRule setupResourceTestRule(List<Object> resourceList, List<Object> filters,
                                                            Map<String, ApiKey> apiKeys,
                                                            Multimap<String, String> permissionsByRole) {
        //noinspection unchecked
        Supplier<String> idSupplier = mock(Supplier.class);
        InMemoryAuthIdentityManager<ApiKey> authIdentityManager = new InMemoryAuthIdentityManager<>(idSupplier);
        for (Map.Entry<String, ApiKey> entry : apiKeys.entrySet()) {
            String key = entry.getKey();
            ApiKey apiKey = entry.getValue();
            when(idSupplier.get()).thenReturn(apiKey.getId());
            authIdentityManager.createIdentity(key,
                    new ApiKeyModification()
                            .withOwner(apiKey.getOwner())
                            .withDescription(apiKey.getDescription())
                            .addRoles(apiKey.getRoles()));
        }

        EmoPermissionResolver permissionResolver = new EmoPermissionResolver(mock(DataStore.class), mock(BlobStore.class));
        InMemoryPermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        RoleManager roleManager = new InMemoryRoleManager(permissionManager);

        for (Map.Entry<String, Collection<String>> entry : permissionsByRole.asMap().entrySet()) {
            createRole(roleManager, null, entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
        }

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



        for (Object filter : filters) {
            resourceTestRuleBuilder.addProvider(filter);
        }



        // Jersey tests don't inject Context parameters, so create an injector to provide a mock instance.
        resourceTestRuleBuilder.addProvider(new DatabusJerseyTest.InjectableContextBinder<>(mock(HttpServletRequest.class)));

        ResourceTestAuthUtil.setUpResources(resourceTestRuleBuilder, SecurityManagerBuilder.create()
                .withAuthIdentityReader(authIdentityManager)
                .withPermissionReader(permissionManager)
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

    /**
     * Convenience method to create a role with permissions.  It only delegates to a single method call, but that call
     * is complex enough and creating roles is done with sufficient frequency that this method is beneficial to
     * maintain readability.
     */
    protected static void createRole(RoleManager roleManager, @Nullable String group, String id, Set<String> permissions) {
        roleManager.createRole(new RoleIdentifier(group, id),
                new RoleModification().withPermissionUpdate(new PermissionUpdateRequest().permit(permissions)));
    }
}
