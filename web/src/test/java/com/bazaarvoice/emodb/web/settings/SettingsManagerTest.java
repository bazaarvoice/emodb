package com.bazaarvoice.emodb.web.settings;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.Cache;
import com.google.inject.util.Providers;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class SettingsManagerTest {

    private DataStore _dataStore;
    private CacheRegistry _cacheRegistry;
    private CacheHandle _cacheHandle;
    private SettingsManager _settingsManager;

    @BeforeMethod
    public void setUp() {
        _dataStore = new InMemoryDataStore(new MetricRegistry());
        _cacheRegistry = mock(CacheRegistry.class);
        _cacheHandle = mock(CacheHandle.class);
        when(_cacheRegistry.register(eq("settings"), any(Cache.class), eq(true))).thenReturn(_cacheHandle);

        _settingsManager = new SettingsManager(Providers.of(_dataStore), "__system:settings",
                "app_global:sys", _cacheRegistry);
    }

    @Test
    public void testSettingFromDefault() {
        Setting<String> setting = _settingsManager.register("test1", String.class, "def");
        assertEquals(setting.getName(), "test1");
        assertEquals(setting.get(), "def");
        // Default is not being read from data store
        assertTrue(Intrinsic.isDeleted(_dataStore.get("__system:settings", "test1")));
    }

    @Test
    public void testDoubleRegistration() {
        Setting<String> setting = _settingsManager.register("test2", String.class, "string");

        // Re-register with same parameters
        assertSame(_settingsManager.register("test2", String.class, "string"), setting);

        // Re-register with different parameters
        try {
            _settingsManager.register("test2", Boolean.class, true);
            fail("type changed");
        } catch (IllegalStateException e) {
            // Ok
        }
        try {
            _settingsManager.register("test2", String.class, "uh-oh");
            fail("Default changed");
        } catch (IllegalStateException e) {
            // OK
        }
    }

    @Test
    public void testCacheInvalidation() {
        Setting<String> setting = _settingsManager.register("test5", String.class, "original");
        assertEquals(setting.get(), "original");
        // Change the value
        setting.set("foo");
        // Time hasn't advanced, so should still be original
        assertEquals(setting.get(), "original");

        // Don't advance time, but invalidate the cache
        verify(_cacheHandle).invalidate(InvalidationScope.GLOBAL, "test5");
        ArgumentCaptor<Cache> captor = ArgumentCaptor.forClass(Cache.class);
        verify(_cacheRegistry).register(eq("settings"), captor.capture(), eq(true));
        captor.getValue().invalidate("test5");

        // Cache should have been invalidated, so new value is now returned
        assertEquals(setting.get(), "foo");
    }
}
