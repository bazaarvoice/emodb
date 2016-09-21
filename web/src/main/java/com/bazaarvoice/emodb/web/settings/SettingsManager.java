package com.bazaarvoice.emodb.web.settings;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.time.ClockTicker;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStoreListener;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.dropwizard.lifecycle.Managed;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Implementations of {@link SettingsRegistry} and {@link Settings} that is backed by a system table.  Each
 * registered setting is stored as a row in the table.  Values are cached for a set time (one minute by default)
 * so reading the values from the returned settings typically do no incur a round trip to Cassandra.
 */
public class SettingsManager implements SettingsRegistry, Settings, Managed {

    private final Logger _log = LoggerFactory.getLogger(getClass());

    /**
     * Setting records written to the data store only have one attribute, which is a JSON representation of the
     * setting's value.
     */
    private final String VALUE_ATTRIBUTE = "json";

    /**
     * Proactively store a settings version.  If the format for stored settings changes or the persisted
     * attributes change then having a version system in-place helps with backwards compatibility.
     */
    private final String VERSION_ATTRIBUTE = "settingVersion";
    private final int CURRENT_SETTING_VERSION = 1;

    /**
     * Changes made in the local data center are propagated immediately using the <code>_lastUpdated</code>
     * value store.  However, if the settings are changed in a remote data center then we do not receive active
     * notification.  For this reason the settings are cached in memory for a limited amount of time before they
     * must be re-fetched from source.
     */
    private final static Duration DEFAULT_CACHE_INVALIDATION_TIME = Duration.standardMinutes(1);

    private final ValueStore<Long> _lastUpdated;
    private final Map<String, RegisteredSetting<?>> _registeredSettings = Maps.newConcurrentMap();
    private final LoadingCache<SettingMetadata<?>, Object> _settingsCache;
    private final Supplier<DataStore> _dataStore;
    private final Supplier<String> _settingsTable;
    private final String _settingsTablePlacement;
    private final Clock _clock;
    private ValueStoreListener _listener;

    /**
     * Injection constructor.  Because of circular dependency issues between the SettingsRegistry interface and the
     * DataStore the latter is injected using its provider.
     */
    @Inject
    public SettingsManager(ValueStore<Long> lastUpdated, Provider<DataStore> dataStoreProvider, String settingsTable,
                           String settingsTablePlacement, LifeCycleRegistry lifeCycleRegistry, Clock clock) {
        this(lastUpdated, dataStoreProvider, settingsTable, settingsTablePlacement, lifeCycleRegistry,
                DEFAULT_CACHE_INVALIDATION_TIME, clock);
    }

    public SettingsManager(ValueStore<Long> lastUpdated, Provider<DataStore> dataStoreProvider, String settingsTable,
                           String settingsTablePlacement, LifeCycleRegistry lifeCycleRegistry,
                           Duration cacheInvalidationTime, Clock clock) {
        _lastUpdated = lastUpdated;
        _dataStore = Suppliers.memoize(dataStoreProvider::get);
        _settingsTablePlacement = settingsTablePlacement;
        _clock = clock;

        _settingsTable = Suppliers.memoize(() -> {
            // Create the settings table if it does not exist
            if (!_dataStore.get().getTableExists(settingsTable)) {
                _dataStore.get().createTable(
                        settingsTable,
                        new TableOptionsBuilder().setPlacement(_settingsTablePlacement).build(),
                        ImmutableMap.<String, Object>of(),
                        new AuditBuilder().setLocalHost().setComment("create settings table").build());
            }
            return settingsTable;
        });

        _settingsCache = CacheBuilder.newBuilder()
                .ticker(ClockTicker.getTicker(_clock))
                .expireAfterWrite(cacheInvalidationTime.getMillis(), TimeUnit.MILLISECONDS)
                .build(new CacheLoader<SettingMetadata<?>, Object>() {
                    @Override
                    public Object load(SettingMetadata<?> metadata) throws Exception {
                        Map<String, Object> valueMap = _dataStore.get().get(
                                _settingsTable.get(), metadata.getName(), ReadConsistency.STRONG);

                        if (Intrinsic.isDeleted(valueMap)) {
                            return metadata.getDefaultValue();
                        }
                        int version = Objects.firstNonNull((Integer) valueMap.get(VERSION_ATTRIBUTE), -1);
                        if (version == CURRENT_SETTING_VERSION) {
                            String rawValue = (String) valueMap.get(VALUE_ATTRIBUTE);
                            return JsonHelper.fromJson(rawValue, metadata.getTypeReference());
                        }
                        throw new IllegalStateException("Setting stored with unparseable version: " + version);
                    }
                });

        lifeCycleRegistry.manage(this);
    }

    @Override
    public void start() throws Exception {
        // Register a listener for locally-sourced settings updates to immediately invalidate the settings cache
        _listener = _settingsCache::invalidateAll;
        _lastUpdated.addListener(_listener);
    }

    @Override
    public void stop() throws Exception {
        _lastUpdated.removeListener(_listener);
    }

    @Override
    public <T> Setting<T> register(String name, Class<T> clazz, T defaultValue) {
        return register(name, asTypeReference(clazz), defaultValue);
    }

    @Override
    public <T> Setting<T> register(String name, TypeReference<T> typeReference, T defaultValue) {
        checkNotNull(name, "name");
        checkNotNull(typeReference, "typeReference");
        checkNotNull(defaultValue, "defaultValue");

        SettingMetadata<T> metadata = new SettingMetadata<>(name, typeReference, defaultValue);
        Setting<T> setting = createSetting(metadata);
        RegisteredSetting newSetting = new RegisteredSetting<>(metadata, setting);

        RegisteredSetting<?> registered = _registeredSettings.putIfAbsent(name, newSetting);
        if (registered != null) {
            checkState(registered.metadata.equals(metadata),
                    "Setting %s already registered with incompatible parameters", name);
            //noinspection unchecked
            return (Setting<T>) registered.setting;
        }
        return setting;
    }

    private <T> Setting<T> createSetting(final SettingMetadata<T> metadata) {
        return new Setting<T>() {
            @Override
            public String getName() {
                return metadata.getName();
            }

            @Override
            public T get() {
                return SettingsManager.this.get(metadata);
            }

            @Override
            public void set(T value) {
                SettingsManager.this.set(metadata, value);
            }
        };
    }

    private <T> T get(SettingMetadata<T> metadata) {
        //noinspection unchecked
        return (T) _settingsCache.getUnchecked(metadata);
    }

    private <T> void set(SettingMetadata<T> metadata, T value) {
        checkNotNull(value, "value");

        Delta delta =  Deltas.mapBuilder()
                .put(VALUE_ATTRIBUTE, JsonHelper.asJson(value))
                .put(VERSION_ATTRIBUTE, CURRENT_SETTING_VERSION)
                .build();

        // Write the delta to the store
        _dataStore.get().update(_settingsTable.get(), metadata.getName(), TimeUUIDs.newUUID(), delta,
                new AuditBuilder().setLocalHost().setComment("Updated setting").build(), WriteConsistency.STRONG);

        // Notify all nodes in the local cluster to refresh immediately; remote clusters will eventually see the update
        try {
            _lastUpdated.set(_clock.millis());
        } catch (Exception e) {
            // This local invalidation was optimistic; log that it failed but otherwise move on.  The local cluster
            // will read the updated value eventually.
            _log.warn("Failed to invalidated settings cache", e);
        }
    }

    @Override
    public <T> Setting<T> getSetting(String name, Class<T> clazz) {
        return getSetting(name, asTypeReference(clazz));
    }

    @Override
    public <T> Setting<T> getSetting(String name, TypeReference<T> typeReference) {
        checkNotNull(name, "name");
        checkNotNull(typeReference, "typeReference");

        RegisteredSetting<?> registered = _registeredSettings.get(name);
        if (registered != null && registered.metadata.getTypeReference().getType().equals(typeReference.getType())) {
            //noinspection unchecked
            return (Setting<T>) registered.setting;
        }
        return null;
    }

    @Override
    public Map<String, Object> getAll() {
        Map<String, Object> settings = Maps.newHashMap();
        for (RegisteredSetting<?> registered : _registeredSettings.values()) {
            settings.put(registered.metadata.getName(), registered.setting.get());
        }
        return settings;
    }

    private <T> TypeReference<T> asTypeReference(Class<T> clazz) {
        checkNotNull(clazz, "class");
        return new TypeReference<T>() {
            @Override
            public Type getType() {
                return clazz;
            }
        };
    }

    // Simple internal class pairing a setting and its metadata
    private final class RegisteredSetting<T> {
        SettingMetadata<T> metadata;
        Setting<T> setting;

        private RegisteredSetting(SettingMetadata<T> metadata, Setting<T> setting) {
            this.metadata = metadata;
            this.setting = setting;
        }
    }
}
