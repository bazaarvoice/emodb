package com.bazaarvoice.emodb.web.auth.apikey;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyAuthenticationToken;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyModification;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.CacheManagingAuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.MatchingPermissionResolver;
import com.bazaarvoice.emodb.auth.permissions.PermissionIDs;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.shiro.GuavaCacheManager;
import com.bazaarvoice.emodb.auth.shiro.InvalidatableCacheManager;
import com.bazaarvoice.emodb.auth.shiro.RolePermissionSet;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.core.DefaultCacheRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.util.LifecycleUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.Set;

import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class ApiKeyRealmTest {

    private AuthIdentityManager<ApiKey> _authIdentityManager;
    private PermissionManager _permissionManager;

    private ApiKeyRealm _underTest;

    @Before
    public void setup() {

        CacheRegistry cacheRegistry = new DefaultCacheRegistry(new SimpleLifeCycleRegistry(), new MetricRegistry());
        InvalidatableCacheManager _cacheManager = new GuavaCacheManager(cacheRegistry);

        InMemoryAuthIdentityManager<ApiKey> authIdentityDAO = new InMemoryAuthIdentityManager<>();
        _authIdentityManager = new CacheManagingAuthIdentityManager<>(authIdentityDAO, _cacheManager);

        _permissionManager = mock(PermissionManager.class);
        MatchingPermissionResolver permissionResolver = new MatchingPermissionResolver();
        when(_permissionManager.getPermissionResolver()).thenReturn(permissionResolver);

        _underTest = new ApiKeyRealm("ApiKeyRealm under test",
                _cacheManager, _authIdentityManager, _permissionManager, null);
        LifecycleUtils.init(_underTest);

//        _permissionCaching.updatePermissions("othertestrole", new PermissionUpdateRequest().permit("city|get|Austin", "country|get|USA"));

    }

    //    @Test
//    public void simpleNull() {
//        assertNotNull(_underTest.getAvailableRolesCache(), "precondition: there is a cache");
//        when(_permissionReader.getPermissions(PermissionIDs.forRole("role"))).thenReturn(null);
//        Collection<Permission> resultPerms = _underTest.getRolePermissions("role");
//        assertNull(resultPerms, "should be no permissions yet");
//    }
//
    @Test
    public void simpleEmpty() {
        assertNotNull(_underTest.getAvailableRolesCache(), "precondition: there is a cache");
        when(_permissionManager.getPermissions(PermissionIDs.forRole("role"))).thenReturn(Sets.<Permission>newHashSet());
        Collection<Permission> resultPerms = _underTest.getRolePermissions("role");
        assertTrue(resultPerms.isEmpty(), "should be no permissions yet");
    }

    @Test
    public void simpleExists() {
        Cache<String, RolePermissionSet> cache = _underTest.getAvailableRolesCache();
        assertEquals(cache.size(), 0, "precondition: cache is empty");
        Permission p1 = mock(Permission.class);
        when(_permissionManager.getPermissions(PermissionIDs.forRole("role"))).thenReturn(Sets.newHashSet(p1));
        Collection<Permission> resultPerms = _underTest.getRolePermissions("role");
        assertNotNull(resultPerms.iterator().next(), "should have a permission");
        assertEquals(cache.size(), 1, "side effect: cache has an element");
    }

    @Test
    public void simpleNewExists() {
        Cache<String, RolePermissionSet> cache = _underTest.getAvailableRolesCache();
        assertEquals(cache.size(), 0, "precondition: cache is empty");
        Permission p1 = mock(Permission.class);
        when(p1.toString()).thenReturn("p1");
        when(_permissionManager.getPermissions(PermissionIDs.forRole("role"))).thenReturn(Sets.newHashSet(p1));
        Collection<Permission> resultPerms = _underTest.getRolePermissions("role");
        assertEquals(resultPerms.iterator().next(), p1, "should have the first permission we added");
        assertEquals(cache.size(), 1, "side effect: cache has one element");
        Permission p2 = mock(Permission.class);
        when(p2.toString()).thenReturn("p2");
        when(_permissionManager.getPermissions(PermissionIDs.forRole("role"))).thenReturn(Sets.newHashSet(p2));
        cache.clear();
        Collection<Permission> resultPerms2 = _underTest.getRolePermissions("role");
        assertEquals(resultPerms2.iterator().next(), p2, "should have the second permission we added");
        assertEquals(cache.size(), 1, "side effect: cache still has one element");
        resultPerms2 = _underTest.getRolePermissions("role");
        assertEquals(resultPerms2.iterator().next(), p2, "should still have the second permission we added");
        assertEquals(cache.size(), 1, "side effect: cache still has one element");
    }

//    @Test
//    public void simpleNowNull() {
//        Cache<String, Set<Permission>> cache = _underTest.getAvailableRolesCache();
//        assertEquals(cache.size(), 0, "precondition: cache is empty");
//        Permission p1 = mock(Permission.class);
//        when(p1.toString()).thenReturn("p1");
//        when(_permissionReader.getRolePermissions("role")).thenReturn(Sets.<Permission>newHashSet(p1));
//        Collection<Permission> resultPerms = _underTest.getRolePermissions("role");
//        assertEquals(resultPerms.iterator().next(), p1, "should have the first permission we added");
//        assertEquals(cache.size(), 1, "side effect: cache has one element");
//        when(_permissionReader.getRolePermissions("role")).thenReturn(null);
//        cache.clear();
//        resultPerms = _underTest.getRolePermissions("role");
//        assertNull(resultPerms, "now should have null");
//        assertEquals(cache.size(), 0, "side effect: cache has nothing");
//    }

    @Test
    public void simpleNowEmpty() {
        Cache<String, RolePermissionSet> cache = _underTest.getAvailableRolesCache();
        assertEquals(cache.size(), 0, "precondition: cache is empty");
        Permission p1 = mock(Permission.class);
        when(p1.toString()).thenReturn("p1");
        when(_permissionManager.getPermissions(PermissionIDs.forRole("role"))).thenReturn(Sets.newHashSet(p1));
        Collection<Permission> resultPerms = _underTest.getRolePermissions("role");
        assertEquals(resultPerms.iterator().next(), p1, "should have the first permission we added");
        assertEquals(cache.size(), 1, "side effect: cache has one element");
        when(_permissionManager.getPermissions(PermissionIDs.forRole("role"))).thenReturn(Sets.<Permission>newHashSet());
        cache.clear();
        resultPerms = _underTest.getRolePermissions("role");
        assertTrue(resultPerms.isEmpty(), "now should have empty");
        assertEquals(cache.size(), 1, "side effect: cache has empty permission");
    }

    @Test
    public void pseudoConcurrentNewExists() {
        Cache<String, RolePermissionSet> cache = _underTest.getAvailableRolesCache();
        assertEquals(cache.size(), 0, "precondition: cache is empty");
        Permission p1 = mock(Permission.class);
        when(p1.toString()).thenReturn("p1");
        Permission p2 = mock(Permission.class);
        when(p2.toString()).thenReturn("p2");
        when(_permissionManager.getPermissions(PermissionIDs.forRole("role"))).thenReturn(Sets.newHashSet(p1), Sets.newHashSet(p2));
        Collection<Permission> resultPerms = _underTest.getRolePermissions("role");
        assertEquals(resultPerms.iterator().next(), p1, "should have the first permission we added");
        assertEquals(cache.size(), 1, "side effect: cache has one element");
        resultPerms = _underTest.getRolePermissions("role");
        assertEquals(resultPerms.iterator().next(), p2, "should have the last permission we added");
        assertEquals(cache.size(), 1, "side effect: cache has one element");
    }

    @Test
    public void pseudoConcurrentNewThenCacheFlush() {
        Cache<String, RolePermissionSet> cache = _underTest.getAvailableRolesCache();
        assertEquals(cache.size(), 0, "precondition: cache is empty");
        Permission p1 = mock(Permission.class);
        when(p1.toString()).thenReturn("p1");
        Permission p2 = mock(Permission.class);
        when(p2.toString()).thenReturn("p2");
        when(_permissionManager.getPermissions(PermissionIDs.forRole("role")))
                .thenReturn(Sets.newHashSet(p1))
                .thenReturn(Sets.newHashSet(p2));
        Collection<Permission> resultPerms = _underTest.getRolePermissions("role");
        assertEquals(resultPerms.iterator().next(), p1, "should have the last permission we added");
        assertEquals(cache.size(), 1, "side effect: cache has one element");
        cache.clear();
        resultPerms = _underTest.getRolePermissions("role");
        assertEquals(resultPerms.iterator().next(), p2, "should again have the last permission we added");
        assertEquals(cache.size(), 1, "side effect: cache again has one element");
    }

    @Test
    public void pseudoConcurrentNewAndCacheFlush() {
        final Cache<String, RolePermissionSet> cache = _underTest.getAvailableRolesCache();
        assertEquals(cache.size(), 0, "precondition: cache is empty");
        final Permission p1 = mock(Permission.class);
        when(p1.toString()).thenReturn("p1");
        final Permission p2 = mock(Permission.class);
        when(p2.toString()).thenReturn("p2");
        when(_permissionManager.getPermissions(PermissionIDs.forRole("role")))
                .thenReturn(Sets.newHashSet(p1))
                .thenAnswer(new Answer<Set<Permission>>() {
                    @Override
                    public Set<Permission> answer(InvocationOnMock invocationOnMock) throws Throwable {
                        cache.clear();
                        return Sets.newHashSet(p2);
                    }
                })
                .thenReturn(Sets.newHashSet(p2));
        Permission resultPerm = _underTest.getRolePermissions("role").iterator().next();
        assertEquals(resultPerm, p1, "should have permission p1");
        resultPerm = _underTest.getRolePermissions("role").iterator().next();
        assertEquals(resultPerm, p2, "should have permission p2");
        resultPerm = _underTest.getRolePermissions("role").iterator().next();
        assertEquals(resultPerm, p2, "should have permission p2");
        assertNotNull(cache.get("role"), "Cached value for role should have been present");
        assertEquals(cache.get("role").permissions(), ImmutableSet.of(p2), "Cached values incorrect");
    }

    @Test
    public void testPermissionCheckById() {
        String id = _authIdentityManager.createIdentity("apikey0", new ApiKeyModification().addRoles("role0"));
        Permission rolePermission = mock(Permission.class);
        Permission positivePermission = mock(Permission.class);
        Permission negativePermission = mock(Permission.class);
        when(rolePermission.implies(positivePermission)).thenReturn(true);
        when(rolePermission.implies(not(eq(positivePermission)))).thenReturn(false);
        when(_permissionManager.getPermissions(PermissionIDs.forRole("role0"))).thenReturn(ImmutableSet.of(rolePermission));

        // Verify the ID is not cached
        assertNull(_underTest.getIdAuthorizationCache().get(id));
        // Verify permission was granted
        assertTrue(_underTest.hasPermissionById(id, positivePermission));
        // Verify the ID was cached
        assertNotNull(_underTest.getIdAuthorizationCache().get(id));
        // Verify no API key information was cached
        assertTrue(_underTest.getAuthenticationCache().keys().isEmpty());
        // Verify permission is granted using the API key
        PrincipalCollection principals = _underTest.getAuthenticationInfo(new ApiKeyAuthenticationToken("apikey0")).getPrincipals();
        assertTrue(_underTest.isPermitted(principals, positivePermission));
        // Negative tests
        assertFalse(_underTest.hasPermissionById(id, negativePermission));
        assertFalse(_underTest.isPermitted(principals, negativePermission));
    }

    @Test
    public void testCachedPermissionCheckById() {
        String id = _authIdentityManager.createIdentity("apikey0", new ApiKeyModification().addRoles("role0"));
        Permission rolePermission = mock(Permission.class);
        Permission positivePermission = mock(Permission.class);
        when(rolePermission.implies(positivePermission)).thenReturn(true);
        when(rolePermission.implies(not(eq(positivePermission)))).thenReturn(false);
        when(_permissionManager.getPermissions(PermissionIDs.forRole("role0"))).thenReturn(ImmutableSet.of(rolePermission));

        // Verify permission is granted using the API key
        PrincipalCollection principals = _underTest.getAuthenticationInfo(new ApiKeyAuthenticationToken("apikey0")).getPrincipals();
        assertTrue(_underTest.isPermitted(principals, positivePermission));
        // Verify the ID was cached
        assertNotNull(_underTest.getIdAuthorizationCache().get(id));
        // Verify permission was granted
        assertTrue(_underTest.hasPermissionById(id, positivePermission));
    }

    @Test
    public void testCachedPermissionCheckByInvalidId() {
        // Verify permission is not granted to a non-existing ID
        assertFalse(_underTest.hasPermissionById("id0", mock(Permission.class)));
        // Verify the ID was cached
        assertNotNull(_underTest.getIdAuthorizationCache().get("id0"));
        // Test again now that the authentication info is cached
        assertFalse(_underTest.hasPermissionById("id0", mock(Permission.class)));
    }
}
