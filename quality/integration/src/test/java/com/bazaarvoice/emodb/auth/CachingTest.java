package com.bazaarvoice.emodb.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyModification;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.CacheManagingAuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.jersey.Authenticated;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.auth.permissions.CacheManagingPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.MatchingPermissionResolver;
import com.bazaarvoice.emodb.auth.permissions.PermissionIDs;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.shiro.GuavaCacheManager;
import com.bazaarvoice.emodb.auth.shiro.InvalidatableCacheManager;
import com.bazaarvoice.emodb.auth.test.ResourceTestAuthUtil;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.core.DefaultCacheRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.function.Supplier;
import javax.ws.rs.client.WebTarget;

import static com.bazaarvoice.emodb.auth.permissions.MatchingPermission.escape;
import static java.lang.String.format;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class CachingTest {

    private static final int INVOCATIONS_PER_ROLE = 2;
    private CacheManagingAuthIdentityManager<ApiKey> _authIdentityCaching;
    private AuthIdentityManager<ApiKey> _authIdentityManager;
    private CacheManagingPermissionManager _permissionCaching;
    private PermissionManager _permissionManager;
    private InvalidatableCacheManager _cacheManager;

    @Rule
    public ResourceTestRule _resourceTestRule = setupResourceTestRule();

    @Path("explicit/country/{country}")
    @Produces(MediaType.TEXT_PLAIN)
    @RequiresAuthentication
    public static class ExplicitPermissionResource {
        @Path("city/{city}")
        @GET
        public String getCity(@PathParam("country") String country, @PathParam("city") String city,
                              @Authenticated Subject subject) {
            if (!subject.hasPermissions(format("country|get|%s", escape(country)), format("city|get|%s", escape(city)))) {
                throw new WebApplicationException(Response.Status.FORBIDDEN);
            }
            return format("Welcome to %s, %s", city, country);
        }
    }

    protected ResourceTestRule setupResourceTestRule() {
        ResourceTestRule.Builder resourceTestRuleBuilder = ResourceTestRule.builder();

        CacheRegistry cacheRegistry = new DefaultCacheRegistry(new SimpleLifeCycleRegistry(), new MetricRegistry());
        _cacheManager = new GuavaCacheManager(cacheRegistry);

        //noinspection unchecked
        Supplier<String> idSupplier = mock(Supplier.class);
        when(idSupplier.get()).thenReturn("id0", "id1").thenThrow(new IllegalStateException("Unexpected createIdentity call"));

        InMemoryAuthIdentityManager<ApiKey> authIdentityDAO = new InMemoryAuthIdentityManager<>(idSupplier);
        _authIdentityCaching = new CacheManagingAuthIdentityManager<>(authIdentityDAO, _cacheManager);
        _authIdentityManager = spy(_authIdentityCaching);

        InMemoryPermissionManager permissionDAO = new InMemoryPermissionManager(new MatchingPermissionResolver());
        _permissionCaching = new CacheManagingPermissionManager(permissionDAO, _cacheManager);
        _permissionManager = spy(_permissionCaching);

        authIdentityDAO.createIdentity("testkey", new ApiKeyModification().addRoles("testrole"));
        authIdentityDAO.createIdentity("othertestkey", new ApiKeyModification().addRoles("testrole"));

        permissionDAO.updatePermissions(PermissionIDs.forRole("testrole"), new PermissionUpdateRequest().permit("city|get|Madrid", "country|get|Spain"));

        ResourceTestAuthUtil.setUpResources(resourceTestRuleBuilder, SecurityManagerBuilder.create()
                .withAuthIdentityReader(_authIdentityManager)
                .withPermissionReader(_permissionManager)
                .withCacheManager(_cacheManager)
                .withAnonymousAccessAs("anon")
                .build());

        resourceTestRuleBuilder.addResource(new ExplicitPermissionResource());

        return resourceTestRuleBuilder.build();
    }

    @Test
    public void testGetOneKey() throws Exception {
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        verify(_authIdentityManager, times(2)).getIdentityByAuthenticationId("testkey");
        verify(_permissionManager, times(INVOCATIONS_PER_ROLE * 1)).getPermissions(PermissionIDs.forRole("testrole"));
        verify(_permissionManager).getPermissionResolver();
        verifyNoMoreInteractions(_permissionManager, _authIdentityManager);
    }

    @Test
    public void testGetTwoKeys() throws Exception {
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        testGetWithMatchingPermissions("othertestkey", "Spain", "Madrid");
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        verify(_authIdentityManager, times(2)).getIdentityByAuthenticationId("testkey");
        verify(_authIdentityManager).getIdentityByAuthenticationId("othertestkey");
        verify(_permissionManager, times(INVOCATIONS_PER_ROLE * 1)).getPermissions(PermissionIDs.forRole("testrole"));
        verify(_permissionManager).getPermissionResolver();
        verifyNoMoreInteractions(_permissionManager, _authIdentityManager);
    }

    @Test
    public void testInvalidateDirect() throws Exception {
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        _cacheManager.invalidateAll();
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        verify(_authIdentityManager, times(4)).getIdentityByAuthenticationId("testkey");
    }

    @Test
    public void testInvalidateByMutation() throws Exception {
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        _permissionCaching.updatePermissions(PermissionIDs.forRole("othertestrole"), new PermissionUpdateRequest().permit("city|get|Austin", "country|get|USA"));
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        _authIdentityCaching.updateIdentity("id1",
                new ApiKeyModification().removeRoles("testrole").addRoles("othertestrole"));
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        verify(_authIdentityManager, times(6)).getIdentityByAuthenticationId("testkey");
        verify(_permissionManager, times(INVOCATIONS_PER_ROLE * 3)).getPermissions(PermissionIDs.forRole("testrole"));
        verify(_permissionManager).getPermissionResolver();
        verifyNoMoreInteractions(_permissionManager, _authIdentityManager);
    }

    @Test
    public void testAddPermissions() throws Exception {
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        testGetWithMissingPermissions("testkey", "USA", "Austin");
        testGetWithMissingPermissions("testkey", "USA", "Austin");
        _permissionCaching.updatePermissions(PermissionIDs.forRole("testrole"), new PermissionUpdateRequest().permit("city|get|Austin", "country|get|USA"));
        testGetWithMatchingPermissions("testkey", "USA", "Austin");
        testGetWithMatchingPermissions("testkey", "USA", "Austin");
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        verify(_authIdentityManager, times(4)).getIdentityByAuthenticationId("testkey");
        verify(_permissionManager, times(INVOCATIONS_PER_ROLE * 2)).getPermissions(PermissionIDs.forRole("testrole"));
        verify(_permissionManager).getPermissionResolver();
        verifyNoMoreInteractions(_permissionManager, _authIdentityManager);
    }

    @Test
    public void testAddUserAndPermissions() throws Exception {
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid"); // +1 testkey, +1 testrole
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");
        testGetWithMatchingPermissions("othertestkey", "Spain", "Madrid"); // +1 othertestkey

        testGetWithMissingPermissions("othertestkey", "USA", "Austin");
        testGetWithMissingPermissions("othertestkey", "USA", "Austin");
        _permissionCaching.updatePermissions(PermissionIDs.forRole("othertestrole"), new PermissionUpdateRequest().permit("city|get|Austin", "country|get|USA"));
        _authIdentityCaching.updateIdentity("id1", new ApiKeyModification().addRoles("othertestrole"));

        testGetWithMatchingPermissions("othertestkey", "USA", "Austin"); // +1 othertestkey, +1 othertestrole
        testGetWithMatchingPermissions("othertestkey", "USA", "Austin");
        testGetWithMatchingPermissions("othertestkey", "Spain", "Madrid"); // +1 testrole
        testGetWithMatchingPermissions("othertestkey", "Spain", "Madrid");
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid"); // +1 testkey
        testGetWithMatchingPermissions("testkey", "Spain", "Madrid");

        verify(_authIdentityManager, times(4)).getIdentityByAuthenticationId("testkey");
        verify(_authIdentityManager, times(4)).getIdentityByAuthenticationId("othertestkey");
        verify(_permissionManager, times(INVOCATIONS_PER_ROLE * 2)).getPermissions(PermissionIDs.forRole("testrole"));
        verify(_permissionManager, times(INVOCATIONS_PER_ROLE * 1)).getPermissions(PermissionIDs.forRole("othertestrole"));
        verify(_permissionManager).getPermissionResolver();
        verifyNoMoreInteractions(_permissionManager, _authIdentityManager);
    }

    private void testGetWithMatchingPermissions(String key, String country, String city) throws Exception {
        Response response = getCountryAndCity(country, city, key);
        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        assertEquals(response.readEntity(String.class), "Welcome to " + city + ", " + country);
    }

    private void testGetWithMissingPermissions(String key, String country, String city) throws Exception {
        Response response = getCountryAndCity(country, city, key);
        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }

    private Response getCountryAndCity(String country, String city, String apiKey) {
        String uri = format("/explicit/country/%s/city/%s", country, city);
        WebTarget resource = _resourceTestRule.client().target(uri);
        if (apiKey != null) {
            return resource.request().header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey).get();
        }
        return resource.request().get();
    }
}
