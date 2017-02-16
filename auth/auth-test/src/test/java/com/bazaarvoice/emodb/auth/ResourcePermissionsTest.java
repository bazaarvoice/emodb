package com.bazaarvoice.emodb.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.jersey.Authenticated;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.MatchingPermissionResolver;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.test.ResourceTestAuthUtil;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.emodb.auth.permissions.MatchingPermission.escape;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

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
                              @QueryParam("moveto") BooleanParam moveto,
                              @Authenticated Subject subject) {
            if (!subject.hasPermissions(format("country|get|%s", escape(country)), format("city|get|%s", escape(city))) || (
                moveto != null && !subject.hasPermissions(format("city|moveto|%s", escape(city)))
            )) {
                throw new WebApplicationException(Response.Status.FORBIDDEN);
            }

            if (moveto == null) {
                return format("Welcome to %s, %s", city, country);
            } else {
                return format("Welcome to your new home in %s, %s", city, country);
            }
        }
    }

    @Path("path/country/{country}")
    @Produces(MediaType.TEXT_PLAIN)
    @RequiresPermissions("country|get|{country}")
    public static class PathPermissionResource {
        @Path("city/{city}")
        @RequiresPermissions("city|get|{city}")
        @GET
        public String getCity(@PathParam("country") String country,
                              @PathParam("city") String city,
                              @ParamRequiresPermissions("city|moveto|{city}") @QueryParam("moveto") BooleanParam moveto
        ) {
            if (moveto != null && moveto.get()) {
                return format("Welcome to your new home in %s, %s", city, country);
            } else {
                return format("Welcome to %s, %s", city, country);
            }
        }
    }

    @Path("query/welcome")
    @Produces(MediaType.TEXT_PLAIN)
    public static class QueryPermissionResource {
        @RequiresPermissions({ "country|get|{?country}", "city|get|{?city}" })
        @GET
        public String getCity(@QueryParam("country") String country,
                              @QueryParam("city") String city,
                              @ParamRequiresPermissions("city|moveto|{?city}") @QueryParam("moveto") BooleanParam moveto
        ) {
            if (moveto != null && moveto.get()) {
                return format("Welcome to your new home in %s, %s", city, country);
            } else {
                return format("Welcome to %s, %s", city, country);
            }
        }
    }

    protected ResourceTestRule setupResourceTestRule() {
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
    public void bruteTestDenyGet() throws Exception {
        _authIdentityDAO.updateIdentity(new ApiKey("misspermKey", "id0", ImmutableSet.of("misspermRole")));
        _permissionDAO.updateForRole("misspermRole", new PermissionUpdateRequest().permit("country|get|Spain"));

        _authIdentityDAO.updateIdentity(new ApiKey("nonMatchingWildKey", "id0", ImmutableSet.of("nonMatchingWildRole")));
        _permissionDAO.updateForRole("nonMatchingWildRole",
            new PermissionUpdateRequest().permit("city|get|Madrid", "country|*|Portugal"));


        final List<Pair<String, String>> keys = ImmutableList.of(
            ImmutablePair.of("missing key", null),
            ImmutablePair.of("non existent identity", "noexistKey"),
            ImmutablePair.of("missing permission", "misspermKey"),
            ImmutablePair.of("non matching wildcard permission", "nonMatchingWildKey")
        );

        final List<String> failures = new ArrayList<>();

        for (Pair<String, String> entry : keys) {
            for (PermissionCheck check : PermissionCheck.values()) {
                {
                    ClientResponse response = getCountryAndCity(check, "Spain", "Madrid", entry.getRight());
                    try {
                        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode(), String.format("key [%s] failed check [%s]", entry.getLeft(), check));
                    } catch (AssertionError e) {
                        failures.add(e.getMessage());
                    }
                }
                {
                    ClientResponse response = movetoCountryAndCity(check, "Spain", "Madrid", entry.getRight());
                    try {
                        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode(), String.format("key [%s] failed moveto check [%s]", entry.getLeft(), check));
                    } catch (AssertionError e) {
                        failures.add(e.getMessage());
                    }
                }
            }
        }

        if (!failures.isEmpty()) {
            fail(String.format("%d failures:\n\t-%s", failures.size(), Joiner.on("\n\t-").join(failures)));
        }
    }

    @Test
    public void bruteTestAcceptGet() throws Exception {
        _authIdentityDAO.updateIdentity(new ApiKey("matchPermKey", "id0", ImmutableSet.of("matchPermRole")));
        _permissionDAO.updateForRole("matchPermRole",
            new PermissionUpdateRequest().permit("city|get|Madrid", "country|get|Spain"));

        _authIdentityDAO.updateIdentity(new ApiKey("matchWildPermKey", "id0", ImmutableSet.of("matchWildPermRole")));
        _permissionDAO.updateForRole("matchWildPermRole",
            new PermissionUpdateRequest().permit("city|get|*", "country|*|*"));

        final List<Pair<String, String>> keys = ImmutableList.of(
            ImmutablePair.of("matching permissions", "matchPermKey"),
            ImmutablePair.of("matching wildcard permissions", "matchWildPermKey")
        );

        final List<String> failures = new ArrayList<>();

        for (Pair<String, String> entry : keys) {
            for (PermissionCheck check : PermissionCheck.values()) {
                {
                    ClientResponse response = getCountryAndCity(check, "Spain", "Madrid", entry.getRight());
                    try {
                        assertEquals(response.getEntity(String.class), "Welcome to Madrid, Spain", String.format("key [%s] failed check [%s]", entry.getLeft(), check));
                    } catch (AssertionError e) {
                        failures.add(e.getMessage());
                    }
                }

                {
                    ClientResponse response = movetoCountryAndCity(check, "Spain", "Madrid", entry.getRight());
                    try {
                        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode(), String.format("key [%s] failed moveto check [%s]", entry.getLeft(), check));
                    } catch (AssertionError e) {
                        failures.add(e.getMessage());
                    }
                }
            }
        }

        if (!failures.isEmpty()) {
            fail(String.format("%d failures:\n\t-%s", failures.size(), Joiner.on("\n\t-").join(failures)));
        }
    }

    @Test
    public void bruteTestAcceptMoveTo() throws Exception {
        _authIdentityDAO.updateIdentity(new ApiKey("matchPermKey", "id0", ImmutableSet.of("matchPermRole")));
        _permissionDAO.updateForRole("matchPermRole",
            new PermissionUpdateRequest().permit("city|get|Madrid", "city|moveto|Madrid", "country|get|Spain"));

        _authIdentityDAO.updateIdentity(new ApiKey("matchWildPermKey", "id0", ImmutableSet.of("matchWildPermRole")));
        _permissionDAO.updateForRole("matchWildPermRole",
            new PermissionUpdateRequest().permit("city|*|*", "country|*|*"));

        final List<Pair<String, String>> keys = ImmutableList.of(
            ImmutablePair.of("matching permissions", "matchPermKey"),
            ImmutablePair.of("matching wildcard permissions", "matchWildPermKey")
        );

        final List<String> failures = new ArrayList<>();

        for (Pair<String, String> entry : keys) {
            for (PermissionCheck check : PermissionCheck.values()) {
                {
                    ClientResponse response = getCountryAndCity(check, "Spain", "Madrid", entry.getRight());
                    try {
                        assertEquals(response.getEntity(String.class), "Welcome to Madrid, Spain", String.format("key [%s] failed check [%s]", entry.getLeft(), check));
                    } catch (AssertionError e) {
                        failures.add(e.getMessage());
                    }
                }

                {
                    ClientResponse response = movetoCountryAndCity(check, "Spain", "Madrid", entry.getRight());
                    try {
                        assertEquals(response.getEntity(String.class), "Welcome to your new home in Madrid, Spain", String.format("key [%s] failed moveto check [%s]", entry.getLeft(), check));
                    } catch (AssertionError e) {
                        failures.add(e.getMessage());
                    }
                }
            }
        }

        if (!failures.isEmpty()) {
            fail(String.format("%d failures:\n\t-%s", failures.size(), Joiner.on("\n\t-").join(failures)));
        }
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
        _authIdentityDAO.updateIdentity(new ApiKey("testkey", "id0", ImmutableSet.of("testrole")));
        _permissionDAO.updateForRole("testrole",
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
        _authIdentityDAO.updateIdentity(new ApiKey("anon", "id1", ImmutableSet.of("anonrole")));
        _permissionDAO.updateForRole("anonrole",
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

    private ClientResponse getCountryAndCity(final PermissionCheck permissionCheck, final String country, final String city, final String apiKey)
        throws Exception {
        final Map<PermissionCheck, String> _uriFormatMap = ImmutableMap.of(
            PermissionCheck.EXPLICIT, "/explicit/country/%s/city/%s",
            PermissionCheck.PATH, "/path/country/%s/city/%s",
            PermissionCheck.QUERY, "/query/welcome?country=%s&city=%s");

        final String uri = format(_uriFormatMap.get(permissionCheck), URLEncoder.encode(country, "UTF-8"), URLEncoder.encode(city, "UTF-8"));
        final WebResource resource = _resourceTestRule.client().resource(uri);
        if (apiKey != null) {
            return resource.header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey).get(ClientResponse.class);
        }
        return resource.get(ClientResponse.class);
    }

    private ClientResponse movetoCountryAndCity(final PermissionCheck permissionCheck, final String country, final String city, final String apiKey)
        throws Exception {
        final Map<PermissionCheck, String> _uriFormatMap = ImmutableMap.of(
            PermissionCheck.EXPLICIT, "/explicit/country/%s/city/%s?moveto=true",
            PermissionCheck.PATH, "/path/country/%s/city/%s?moveto=true",
            PermissionCheck.QUERY, "/query/welcome?country=%s&city=%s&moveto=true");

        final String uri = format(_uriFormatMap.get(permissionCheck), URLEncoder.encode(country, "UTF-8"), URLEncoder.encode(city, "UTF-8"));
        final WebResource resource = _resourceTestRule.client().resource(uri);
        if (apiKey != null) {
            return resource.header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey).get(ClientResponse.class);
        }
        return resource.get(ClientResponse.class);
    }
}
