package com.bazaarvoice.emodb.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyModification;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.jersey.Authenticated;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.MatchingPermissionResolver;
import com.bazaarvoice.emodb.auth.permissions.PermissionIDs;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.test.ResourceTestAuthUtil;
import com.bazaarvoice.emodb.web.auth.SecurityManagerBuilder;
import com.google.common.collect.ImmutableMap;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URLEncoder;
import java.util.Map;

import static com.bazaarvoice.emodb.auth.permissions.MatchingPermission.escape;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class ResourcePermissionsTest {

    private InMemoryAuthIdentityManager<ApiKey> _authIdentityDAO = new InMemoryAuthIdentityManager<>();
    private InMemoryPermissionManager _permissionDAO = new InMemoryPermissionManager(new MatchingPermissionResolver());

    @Rule
    public ResourceTestRule _resourceTestRule = setupResourceTestRule();

    private enum PermissionCheck {
        EXPLICIT,
        PATH,
        QUERY
    }

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

    @Path("path/country/{country}")
    @Produces(MediaType.TEXT_PLAIN)
    @RequiresPermissions("country|get|{country}")
    public static class PathPermissionResource {
        @Path("city/{city}")
        @RequiresPermissions("city|get|{city}")
        @GET
        public String getCity(@PathParam("country") String country, @PathParam("city") String city) {
            return format("Welcome to %s, %s", city, country);
        }
    }

    @Path("query/welcome")
    @Produces(MediaType.TEXT_PLAIN)
    public static class QueryPermissionResource {
        @RequiresPermissions({"country|get|{?country}", "city|get|{?city}"})
        @GET
        public String getCity(@QueryParam("country") String country, @QueryParam("city") String city) {
            return format("Welcome to %s, %s", city, country);
        }
    }

    protected ResourceTestRule setupResourceTestRule() {
        ResourceTestRule.Builder resourceTestRuleBuilder = ResourceTestRule.builder();

        ResourceTestAuthUtil.setUpResources(resourceTestRuleBuilder, SecurityManagerBuilder.create()
                .withAuthIdentityReader(_authIdentityDAO)
                .withPermissionReader(_permissionDAO)
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
    public void testGetWithMissingIdentityExplicit() throws Exception {
        testGetWithMissingIdentity(PermissionCheck.EXPLICIT);
    }

    @Test
    public void testGetWithMissingIdentityPath() throws Exception {
        testGetWithMissingIdentity(PermissionCheck.PATH);
    }

    @Test
    public void testGetWithMissingIdentityQuery() throws Exception {
        testGetWithMissingIdentity(PermissionCheck.QUERY);
    }

    private void testGetWithMissingIdentity(PermissionCheck permissionCheck) throws Exception {
        ClientResponse response = getCountryAndCity(permissionCheck, "Spain", "Madrid", null);
        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }

    @Test
    public void testGetWithNonExistentIdentityExplicit() throws Exception {
        testGetWithNonExistentIdentity(PermissionCheck.EXPLICIT);
    }

    @Test
    public void testGetWithNonExistentIdentityPath() throws Exception {
        testGetWithNonExistentIdentity(PermissionCheck.PATH);
    }

    @Test
    public void testGetWithNonExistentIdentityQuery() throws Exception {
        testGetWithNonExistentIdentity(PermissionCheck.QUERY);
    }

    private void testGetWithNonExistentIdentity(PermissionCheck permissionCheck) throws Exception {
        ClientResponse response = getCountryAndCity(permissionCheck, "Spain", "Madrid", "testkey");
        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }

    @Test
    public void testGetWithMissingPermissionExplicit() throws Exception {
        testGetWithMissingPermission(PermissionCheck.EXPLICIT);
    }

    @Test
    public void testGetWithMissingPermissionPath() throws Exception {
        testGetWithMissingPermission(PermissionCheck.PATH);
    }

    @Test
    public void testGetWithMissingPermissionQuery() throws Exception {
        testGetWithMissingPermission(PermissionCheck.QUERY);
    }

    private void testGetWithMissingPermission(PermissionCheck permissionCheck) throws Exception {
        _authIdentityDAO.createIdentity("testkey", new ApiKeyModification().addRoles("testrole"));
        _permissionDAO.updatePermissions("testrole", new PermissionUpdateRequest().permit("country|get|Spain"));

        ClientResponse response = getCountryAndCity(permissionCheck, "Spain", "Madrid", "testkey");
        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }

    @Test
    public void testGetWithMatchingPermissionsExplicit() throws Exception {
        testGetWithMatchingPermissions(PermissionCheck.EXPLICIT);
    }

    @Test
    public void testGetWithMatchingPermissionsPath() throws Exception {
        testGetWithMatchingPermissions(PermissionCheck.PATH);
    }

    @Test
    public void testGetWithMatchingPermissionsQuery() throws Exception {
        testGetWithMatchingPermissions(PermissionCheck.QUERY);
    }

    private void testGetWithMatchingPermissions(PermissionCheck permissionCheck) throws Exception {
        _authIdentityDAO.createIdentity("testkey", new ApiKeyModification().addRoles("testrole"));
        _permissionDAO.updatePermissions(PermissionIDs.forRole("testrole"),
                new PermissionUpdateRequest().permit("city|get|Madrid", "country|get|Spain"));

        ClientResponse response = getCountryAndCity(permissionCheck, "Spain", "Madrid", "testkey");
        assertEquals(response.getEntity(String.class), "Welcome to Madrid, Spain");
    }

    @Test
    public void testGetWithMatchingWildcardPermissionsExplicit() throws Exception {
        testGetWithMatchingWildcardPermissions(PermissionCheck.EXPLICIT);
    }

    @Test
    public void testGetWithMatchingWildcardPermissionsPath() throws Exception {
        testGetWithMatchingWildcardPermissions(PermissionCheck.PATH);
    }

    @Test
    public void testGetWithMatchingWildcardPermissionsQuery() throws Exception {
        testGetWithMatchingWildcardPermissions(PermissionCheck.QUERY);
    }

    private void testGetWithMatchingWildcardPermissions(PermissionCheck permissionCheck) throws Exception {
        _authIdentityDAO.createIdentity("testkey", new ApiKeyModification().addRoles("testrole"));
        _permissionDAO.updatePermissions(PermissionIDs.forRole("testrole"),
                new PermissionUpdateRequest().permit("city|get|*", "country|*|*"));

        ClientResponse response = getCountryAndCity(permissionCheck, "Spain", "Madrid", "testkey");
        assertEquals(response.getEntity(String.class), "Welcome to Madrid, Spain");
    }

    @Test
    public void testGetWithNonMatchingWildcardPermissionExplicit() throws Exception {
        testGetWithNonMatchingWildcardPermission(PermissionCheck.EXPLICIT);
    }

    @Test
    public void testGetWithNonMatchingWildcardPermissionPath() throws Exception {
        testGetWithNonMatchingWildcardPermission(PermissionCheck.PATH);
    }

    @Test
    public void testGetWithNonMatchingWildcardPermissionQuery() throws Exception {
        testGetWithNonMatchingWildcardPermission(PermissionCheck.QUERY);
    }

    private void testGetWithNonMatchingWildcardPermission(PermissionCheck permissionCheck) throws Exception {
        _authIdentityDAO.createIdentity("testkey", new ApiKeyModification().addRoles("testrole"));
        _permissionDAO.updatePermissions(PermissionIDs.forRole("testrole"),
                new PermissionUpdateRequest().permit("city|get|Madrid", "country|*|Portugal"));

        ClientResponse response = getCountryAndCity(permissionCheck, "Spain", "Madrid", "testkey");
        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }

    @Test
    public void testGetWithEscapedPermissionExplicit() throws Exception {
        testGetWithEscapedPermission(PermissionCheck.EXPLICIT);
    }

    @Test
    public void testGetWithEscapedPermissionPath() throws Exception {
        testGetWithEscapedPermission(PermissionCheck.PATH);
    }

    @Test
    public void testGetWithEscapedPermissionQuery() throws Exception {
        testGetWithEscapedPermission(PermissionCheck.QUERY);
    }

    private void testGetWithEscapedPermission(PermissionCheck permissionCheck) throws Exception {
        _authIdentityDAO.createIdentity("testkey", new ApiKeyModification().addRoles("testrole"));
        _permissionDAO.updatePermissions(PermissionIDs.forRole("testrole"),
                new PermissionUpdateRequest().permit("city|get|Pipe\\|Town", "country|get|Star\\*Nation"));

        ClientResponse response = getCountryAndCity(permissionCheck, "Star*Nation", "Pipe|Town", "testkey");
        assertEquals(response.getEntity(String.class), "Welcome to Pipe|Town, Star*Nation");
    }

    @Test
    public void testAnonymousWithPermissionExplicit() throws Exception {
        testAnonymousWithPermission(PermissionCheck.EXPLICIT);
    }

    @Test
    public void testAnonymousWithPermissionPath() throws Exception {
        testAnonymousWithPermission(PermissionCheck.PATH);
    }

    @Test
    public void testAnonymousWithPermissionQuery() throws Exception {
        testAnonymousWithPermission(PermissionCheck.QUERY);
    }

    private void testAnonymousWithPermission(PermissionCheck permissionCheck) throws Exception {
        _authIdentityDAO.createIdentity("anon", new ApiKeyModification().addRoles("anonrole"));
        _permissionDAO.updatePermissions(PermissionIDs.forRole("anonrole"),
                new PermissionUpdateRequest().permit("city|get|Madrid", "country|get|Spain"));

        ClientResponse response = getCountryAndCity(permissionCheck, "Spain", "Madrid", null);
        assertEquals(response.getEntity(String.class), "Welcome to Madrid, Spain");
    }

    @Test
    public void testAnonymousWithoutPermissionExplicit() throws Exception {
        testAnonymousWithoutPermission(PermissionCheck.EXPLICIT);
    }

    @Test
    public void testAnonymousWithoutPermissionQuery() throws Exception {
        testAnonymousWithoutPermission(PermissionCheck.PATH);
    }

    @Test
    public void testAnonymousWithoutPermissionPath() throws Exception {
        testAnonymousWithoutPermission(PermissionCheck.QUERY);
    }

    private void testAnonymousWithoutPermission(PermissionCheck permissionCheck) throws Exception {
        ClientResponse response = getCountryAndCity(permissionCheck, "Spain", "Madrid", null);
        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }

    private Map<PermissionCheck, String> _uriFormatMap = ImmutableMap.of(
            PermissionCheck.EXPLICIT, "/explicit/country/%s/city/%s",
            PermissionCheck.PATH, "/path/country/%s/city/%s",
            PermissionCheck.QUERY, "/query/welcome?country=%s&city=%s");

    private ClientResponse getCountryAndCity(PermissionCheck permissionCheck, String country, String city, String apiKey)
            throws Exception {
        String uri = format(_uriFormatMap.get(permissionCheck), URLEncoder.encode(country, "UTF-8"), URLEncoder.encode(city, "UTF-8"));
        WebResource resource = _resourceTestRule.client().resource(uri);
        if (apiKey != null) {
            return resource.header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey).get(ClientResponse.class);
        }
        return resource.get(ClientResponse.class);
    }
}
