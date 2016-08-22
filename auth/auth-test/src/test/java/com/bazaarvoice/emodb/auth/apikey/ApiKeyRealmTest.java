package com.bazaarvoice.emodb.auth.apikey;

import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.CacheManagingAuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.MatchingPermissionResolver;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.shiro.GuavaCacheManager;
import com.bazaarvoice.emodb.auth.shiro.InvalidatableCacheManager;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.core.DefaultCacheRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.cache.Cache;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class ApiKeyRealmTest {

    private PermissionManager _permissionManager;

    private ApiKeyRealm _underTest;

    @Before
    public void setup() {

        CacheRegistry cacheRegistry = new DefaultCacheRegistry(new SimpleLifeCycleRegistry(), new MetricRegistry());
        InvalidatableCacheManager _cacheManager = new GuavaCacheManager(cacheRegistry);

        InMemoryAuthIdentityManager<ApiKey> authIdentityDAO = new InMemoryAuthIdentityManager<>();
        AuthIdentityManager<ApiKey> _authIdentityManager = new CacheManagingAuthIdentityManager<>(authIdentityDAO, _cacheManager);

        _permissionManager = mock(PermissionManager.class);
        MatchingPermissionResolver permissionResolver = new MatchingPermissionResolver();
        when(_permissionManager.getPermissionResolver()).thenReturn(permissionResolver);

        _underTest = new ApiKeyRealm("ApiKeyRealm under test",
                _cacheManager, _authIdentityManager, _permissionManager, null);

//        _permissionCaching.updateForRole("othertestrole", new PermissionUpdateRequest().permit("city|get|Austin", "country|get|USA"));

    }

//    @Test
//    public void simpleNull() {
//        assertNotNull(_underTest.getAvailableRolesCache(), "precondition: there is a cache");
//        when(_permissionManager.getAllForRole("role")).thenReturn(null);
//        Collection<Permission> resultPerms = _underTest.getPermissions("role");
//        assertNull(resultPerms, "should be no permissions yet");
//    }
//
    @Test
    public void simpleEmpty() {
        assertNotNull(_underTest.getAvailableRolesCache(), "precondition: there is a cache");
        when(_permissionManager.getAllForRole("role")).thenReturn(Sets.<Permission>newHashSet());
        Collection<Permission> resultPerms = _underTest.getPermissions("role");
        assertTrue(resultPerms.isEmpty(), "should be no permissions yet");
    }

    @Test
    public void simpleExists() {
        Cache<String, Set<Permission>> cache = _underTest.getAvailableRolesCache();
        assertEquals(cache.size(), 0, "precondition: cache is empty");
        Permission p1 = mock(Permission.class);
        when(_permissionManager.getAllForRole("role")).thenReturn(Sets.newHashSet(p1));
        Collection<Permission> resultPerms = _underTest.getPermissions("role");
        assertNotNull(resultPerms.iterator().next(), "should have a permission");
        assertEquals(cache.size(), 1, "side effect: cache has an element");
    }

    @Test
    public void simpleNewExists() {
        Cache<String, Set<Permission>> cache = _underTest.getAvailableRolesCache();
        assertEquals(cache.size(), 0, "precondition: cache is empty");
        Permission p1 = mock(Permission.class);
        when(p1.toString()).thenReturn("p1");
        when(_permissionManager.getAllForRole("role")).thenReturn(Sets.newHashSet(p1));
        Collection<Permission> resultPerms = _underTest.getPermissions("role");
        assertEquals(resultPerms.iterator().next(), p1, "should have the first permission we added");
        assertEquals(cache.size(), 1, "side effect: cache has one element");
        Permission p2 = mock(Permission.class);
        when(p2.toString()).thenReturn("p2");
        when(_permissionManager.getAllForRole("role")).thenReturn(Sets.newHashSet(p2));
        cache.clear();
        Collection<Permission> resultPerms2 = _underTest.getPermissions("role");
        assertEquals(resultPerms2.iterator().next(), p2, "should have the second permission we added");
        assertEquals(cache.size(), 1, "side effect: cache still has one element");
        resultPerms2 = _underTest.getPermissions("role");
        assertEquals(resultPerms2.iterator().next(), p2, "should still have the second permission we added");
        assertEquals(cache.size(), 1, "side effect: cache still has one element");
    }

//    @Test
//    public void simpleNowNull() {
//        Cache<String, Set<Permission>> cache = _underTest.getAvailableRolesCache();
//        assertEquals(cache.size(), 0, "precondition: cache is empty");
//        Permission p1 = mock(Permission.class);
//        when(p1.toString()).thenReturn("p1");
//        when(_permissionManager.getAllForRole("role")).thenReturn(Sets.<Permission>newHashSet(p1));
//        Collection<Permission> resultPerms = _underTest.getPermissions("role");
//        assertEquals(resultPerms.iterator().next(), p1, "should have the first permission we added");
//        assertEquals(cache.size(), 1, "side effect: cache has one element");
//        when(_permissionManager.getAllForRole("role")).thenReturn(null);
//        cache.clear();
//        resultPerms = _underTest.getPermissions("role");
//        assertNull(resultPerms, "now should have null");
//        assertEquals(cache.size(), 0, "side effect: cache has nothing");
//    }

    @Test
    public void simpleNowEmpty() {
        Cache<String, Set<Permission>> cache = _underTest.getAvailableRolesCache();
        assertEquals(cache.size(), 0, "precondition: cache is empty");
        Permission p1 = mock(Permission.class);
        when(p1.toString()).thenReturn("p1");
        when(_permissionManager.getAllForRole("role")).thenReturn(Sets.newHashSet(p1));
        Collection<Permission> resultPerms = _underTest.getPermissions("role");
        assertEquals(resultPerms.iterator().next(), p1, "should have the first permission we added");
        assertEquals(cache.size(), 1, "side effect: cache has one element");
        when(_permissionManager.getAllForRole("role")).thenReturn(Sets.<Permission>newHashSet());
        cache.clear();
        resultPerms = _underTest.getPermissions("role");
        assertTrue(resultPerms.isEmpty(), "now should have empty");
        assertEquals(cache.size(), 1, "side effect: cache has empty permission");
    }

    @Test
    public void pseudoConcurrentNewExists() {
        Cache<String, Set<Permission>> cache = _underTest.getAvailableRolesCache();
        assertEquals(cache.size(), 0, "precondition: cache is empty");
        Permission p1 = mock(Permission.class);
        when(p1.toString()).thenReturn("p1");
        Permission p2 = mock(Permission.class);
        when(p2.toString()).thenReturn("p2");
        when(_permissionManager.getAllForRole("role")).thenReturn(Sets.newHashSet(p1), Sets.newHashSet(p2));
        Collection<Permission> resultPerms = _underTest.getPermissions("role");
        assertEquals(resultPerms.iterator().next(), p2, "should have the last permission we added");
        assertEquals(cache.size(), 1, "side effect: cache has one element");
    }

    @Test
    public void pseudoConcurrentNewThenCacheFlush() {
        Cache<String, Set<Permission>> cache = _underTest.getAvailableRolesCache();
        assertEquals(cache.size(), 0, "precondition: cache is empty");
        Permission p1 = mock(Permission.class);
        when(p1.toString()).thenReturn("p1");
        Permission p2 = mock(Permission.class);
        when(p2.toString()).thenReturn("p2");
        when(_permissionManager.getAllForRole("role"))
                .thenReturn(Sets.newHashSet(p1))
                .thenReturn(Sets.newHashSet(p2));
        Collection<Permission> resultPerms = _underTest.getPermissions("role");
        assertEquals(resultPerms.iterator().next(), p2, "should have the last permission we added");
        assertEquals(cache.size(), 1, "side effect: cache has one element");
        cache.clear();
        resultPerms = _underTest.getPermissions("role");
        assertEquals(resultPerms.iterator().next(), p2, "should again have the last permission we added");
        assertEquals(cache.size(), 1, "side effect: cache again has one element");
    }

    @Test
    public void pseudoConcurrentNewAndCacheFlush() {
        final Cache<String, Set<Permission>> cache = _underTest.getAvailableRolesCache();
        assertEquals(cache.size(), 0, "precondition: cache is empty");
        final Permission p1 = mock(Permission.class);
        when(p1.toString()).thenReturn("p1");
        final Permission p2 = mock(Permission.class);
        when(p2.toString()).thenReturn("p2");
        when(_permissionManager.getAllForRole("role"))
                .thenReturn(Sets.newHashSet(p1))
                .thenAnswer(new Answer() {
                    @Override
                    public Object answer(InvocationOnMock invocation) {
                        cache.clear();
                        return Sets.newHashSet(p2);
                    }
                })
                .thenReturn(Sets.newHashSet(p2));
        Permission resultPerm = _underTest.getPermissions("role").iterator().next();
        assertEquals(resultPerm, p2, "should have the last permission we added");
        assertNotEquals(resultPerm, p1, "sanity check");
        assertEquals(cache.size(), 1, "side effect: cache has one element");
    }
}
