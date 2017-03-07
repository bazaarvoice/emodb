package com.bazaarvoice.emodb.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.identity.IdentityState;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.jersey.Authenticated;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.MatchingPermissionResolver;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.test.ResourceTestAuthUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URLEncoder;
import java.util.Collection;

import static com.bazaarvoice.emodb.auth.permissions.MatchingPermission.escape;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ResourcePermissionsTest {

    private InMemoryAuthIdentityManager<ApiKey> _authIdentityDAO = new InMemoryAuthIdentityManager<>(ApiKey.class);
    private InMemoryPermissionManager _permissionDAO = new InMemoryPermissionManager(new MatchingPermissionResolver());

    private PermissionCheck _permissionCheck;

    public ResourcePermissionsTest(PermissionCheck permissionCheck) {
        _permissionCheck = permissionCheck;
    }

    @Rule
    public ResourceTestRule _resourceTestRule = setupResourceTestRule();

    /**
     * Run each test using an endpoint that either uses an explicit permission check, an implicit permission check based
     * on path values, or an implicit permission check based on query parameters.
     */
    private enum PermissionCheck {
        EXPLICIT("/explicit/country/%s/city/%s"),
        PATH("/path/country/%s/city/%s"),
        QUERY("/query/welcome?country=%s&city=%s");

        private String uriFormat;

        PermissionCheck(String format) {
            this.uriFormat = format;
        }

        public String getUri(String country, String city) throws Exception {
            return format(uriFormat, URLEncoder.encode(country, "UTF-8"), URLEncoder.encode(city, "UTF-8"));
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> permissionChecks() {
        return ImmutableList.of(
                new Object[] { PermissionCheck.EXPLICIT },
                new Object[] { PermissionCheck.PATH },
                new Object[] { PermissionCheck.QUERY });
    }

    @Path ("explicit/country/{country}")
    @Produces (MediaType.TEXT_PLAIN)
    @RequiresAuthentication
    public static class ExplicitPermissionResource {
        @Path ("city/{city}")
        @GET
        public String getCity(@PathParam ("country") String country, @PathParam("city") String city,
                              @Authenticated Subject subject) {
            if (!subject.hasPermissions(format("country|get|%s", escape(country)), format("city|get|%s", escape(city)))) {
                throw new WebApplicationException(Response.Status.FORBIDDEN);
            }
            return format("Welcome to %s, %s", city, country);
        }
    }

    @Path ("path/country/{country}")
    @Produces (MediaType.TEXT_PLAIN)
    @RequiresPermissions ("country|get|{country}")
    public static class PathPermissionResource {
        @Path ("city/{city}")
        @RequiresPermissions("city|get|{city}")
        @GET
        public String getCity(@PathParam("country") String country, @PathParam("city") String city) {
            return format("Welcome to %s, %s", city, country);
        }
    }

    @Path ("query/welcome")
    @Produces (MediaType.TEXT_PLAIN)
    public static class QueryPermissionResource {
        @RequiresPermissions({"country|get|{?country}", "city|get|{?city}"})
        @GET
        public String getCity(@QueryParam ("country") String country, @QueryParam("city") String city) {
            return format("Welcome to %s, %s", city, country);
        }
    }

    protected ResourceTestRule setupResourceTestRule()  {
        ResourceTestRule.Builder resourceTestRuleBuilder = ResourceTestRule.builder();

        ResourceTestAuthUtil.setUpResources(resourceTestRuleBuilder, SecurityManagerBuilder.create()
                .withAuthIdentityManager(_authIdentityDAO)
                .withPermissionManager(_permissionDAO)
                .withAnonymousAccessAs("anon")
                .build());

        resourceTestRuleBuilder.addResource(new ExplicitPermissionResource());
        resourceTestRuleBuilder.addResource(new PathPermissionResource());
        resourceTestRuleBuilder.addResource(new QueryPermissionResource());

        return resourceTestRuleBuilder.build();
    }

    @After
    public void cleanupTest() {
        _authIdentityDAO.reset();
        _permissionDAO.reset();
    }

    @Test
    public void testGetWithMissingIdentity() throws Exception {
        ClientResponse response = getCountryAndCity("Spain", "Madrid", null);
        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }

    @Test
    public void testGetWithNonExistentIdentity() throws Exception {
        ClientResponse response = getCountryAndCity("Spain", "Madrid", "testkey");
        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }

    @Test
    public void testGetWithMissingPermission() throws Exception {
        _authIdentityDAO.updateIdentity(new ApiKey("testkey", "id0", IdentityState.ACTIVE, ImmutableSet.of("testrole")));
        _permissionDAO.updateForRole("testrole", new PermissionUpdateRequest().permit("country|get|Spain"));

        ClientResponse response = getCountryAndCity("Spain", "Madrid", "testkey");
        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }

    @Test
    public void testGetWithMatchingPermissions() throws Exception {
        _authIdentityDAO.updateIdentity(new ApiKey("testkey", "id0", IdentityState.ACTIVE, ImmutableSet.of("testrole")));
        _permissionDAO.updateForRole("testrole",
                new PermissionUpdateRequest().permit("city|get|Madrid", "country|get|Spain"));

        ClientResponse response = getCountryAndCity("Spain", "Madrid", "testkey");
        assertEquals(response.getEntity(String.class), "Welcome to Madrid, Spain");
    }

    @Test
    public void testGetWithMatchingWildcardPermissions() throws Exception {
        _authIdentityDAO.updateIdentity(new ApiKey("testkey", "id0", IdentityState.ACTIVE, ImmutableSet.of("testrole")));
        _permissionDAO.updateForRole("testrole",
                new PermissionUpdateRequest().permit("city|get|*", "country|*|*"));

        ClientResponse response = getCountryAndCity("Spain", "Madrid", "testkey");
        assertEquals(response.getEntity(String.class), "Welcome to Madrid, Spain");
    }

    @Test
    public void testGetWithNonMatchingWildcardPermission() throws Exception {
        _authIdentityDAO.updateIdentity(new ApiKey("testkey", "id0", IdentityState.ACTIVE, ImmutableSet.of("testrole")));
        _permissionDAO.updateForRole("testrole",
                new PermissionUpdateRequest().permit("city|get|Madrid", "country|*|Portugal"));

        ClientResponse response = getCountryAndCity("Spain", "Madrid", "testkey");
        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }

    @Test
    public void testGetWithEscapedPermission() throws Exception {
        _authIdentityDAO.updateIdentity(new ApiKey("testkey", "id0", IdentityState.ACTIVE, ImmutableSet.of("testrole")));
        _permissionDAO.updateForRole("testrole",
                new PermissionUpdateRequest().permit("city|get|Pipe\\|Town", "country|get|Star\\*Nation"));

        ClientResponse response = getCountryAndCity("Star*Nation", "Pipe|Town", "testkey");
        assertEquals(response.getEntity(String.class), "Welcome to Pipe|Town, Star*Nation");
    }

    @Test
    public void testGetWithInactiveAPIKey() throws Exception {
        // API key has role that can do anything
        _authIdentityDAO.updateIdentity(new ApiKey("testkey", "id0", IdentityState.ACTIVE, ImmutableSet.of("testrole")));
        _permissionDAO.updateForRole("testrole", new PermissionUpdateRequest().permit("*"));

        ClientResponse response = getCountryAndCity("Spain", "Madrid", "testkey");
        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        // Inactivate the API key and try again
        _authIdentityDAO.updateIdentity(new ApiKey("testkey", "id0", IdentityState.INACTIVE, ImmutableSet.of("testrole")));
        response = getCountryAndCity("Spain", "Madrid", "testkey");
        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }

    @Test
    public void testAnonymousWithPermission() throws Exception {
        _authIdentityDAO.updateIdentity(new ApiKey("anon", "id1", IdentityState.ACTIVE, ImmutableSet.of("anonrole")));
        _permissionDAO.updateForRole("anonrole",
                new PermissionUpdateRequest().permit("city|get|Madrid", "country|get|Spain"));

        ClientResponse response = getCountryAndCity("Spain", "Madrid", null);
        assertEquals(response.getEntity(String.class), "Welcome to Madrid, Spain");
    }

    @Test
    public void testAnonymousWithoutPermission() throws Exception {
        ClientResponse response = getCountryAndCity("Spain", "Madrid", null);
        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }

    private ClientResponse getCountryAndCity(String country, String city, String apiKey)
            throws Exception {
        String uri = _permissionCheck.getUri(country, city);
        WebResource resource = _resourceTestRule.client().resource(uri);
        if (apiKey != null) {
            return resource.header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey).get(ClientResponse.class);
        }
        return resource.get(ClientResponse.class);
    }
}
