package com.bazaarvoice.emodb.web.settings;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStoreListener;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Ticker;
import org.joda.time.Duration;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class SettingsManagerTest {

    private final static long START_TIME_NS = 1472766259192000000L;

    private DataStore _dataStore;
    private ValueStore<Long> _lastUpdated;
    private Ticker _ticker;
    private SettingsManager _settingsManager;

    @BeforeMethod
    public void setUp() {
        _dataStore = new InMemoryDataStore(new MetricRegistry());
        _lastUpdated = mock(ValueStore.class);
        _ticker = mock(Ticker.class);
        when(_ticker.read()).thenReturn(START_TIME_NS);

        _settingsManager = new SettingsManager(_lastUpdated, _dataStore, "__system:settings",
                "app_global:sys", mock(LifeCycleRegistry.class), Duration.standardMinutes(1), _ticker);
    }

    @Test
    public void testSetting() {
        Setting<String> setting = _settingsManager.register("test1", String.class, "def");
        assertEquals(setting.getName(), "test1");
        assertEquals(setting.get(), "def");
        // Default is not being read from data store
        assertTrue(Intrinsic.isDeleted(_dataStore.get("__system:settings", "test1")));

        // Update value
        setting.set("newvalue");
        // Need to advance time by 1 minute to enforce value returned is not cached
        when(_ticker.read()).thenReturn(START_TIME_NS + TimeUnit.MINUTES.toNanos(1));
        assertEquals(setting.get(), "newvalue");
        assertEquals(_dataStore.get("__system:settings", "test1").get("json"), "\"newvalue\"");

        // Verify setting exists in manager
        assertSame(_settingsManager.getSetting("test1", String.class), setting);
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
    public void testCaching() {
        Setting<String> setting = _settingsManager.register("test3", String.class, "original");
        assertEquals(setting.get(), "original");

        // Update the value in the backend without going through the settings API
        _dataStore.update("__system:settings", "test3", TimeUUIDs.newUUID(),
                Deltas.mapBuilder().put("json", "\"newvalue\"").put("settingVersion", 1).build(),
                new AuditBuilder().setComment("test").build());

        for (int sec : new Integer[] {0, 1, 59, 60, 120}) {
            when(_ticker.read()).thenReturn(START_TIME_NS + TimeUnit.SECONDS.toNanos(sec));
            assertEquals(setting.get(), sec < 60 ? "original" : "newvalue");
        }
    }

    @Test
    public void testUpdateNotification() throws Exception {
        Setting<String> setting = _settingsManager.register("test4", String.class, "original");
        setting.set("foo");
        verify(_lastUpdated).set(TimeUnit.NANOSECONDS.toMillis(START_TIME_NS));
    }

    @Test
    public void testCacheInvalidation() throws Exception {
        _settingsManager.start();
        ArgumentCaptor<ValueStoreListener> captor = ArgumentCaptor.forClass(ValueStoreListener.class);
        verify(_lastUpdated).addListener(captor.capture());

        Setting<String> setting = _settingsManager.register("test5", String.class, "original");
        assertEquals(setting.get(), "original");
        // Change the value
        setting.set("foo");
        // Time hasn't advanced, so should still be original
        assertEquals(setting.get(), "original");

        // Don't advance time, but call the value store listener
        captor.getValue().valueChanged();

        // Cache should have been invalidated, so new value is now returned
        assertEquals(setting.get(), "foo");
    }
}
