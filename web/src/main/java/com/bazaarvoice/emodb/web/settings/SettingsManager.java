package com.bazaarvoice.emodb.web.settings;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
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
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
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
     * Changes made in the local data center are propagated immediately using the <code>_lastUpdated</code>
     * value store.  However, if the settings are changed in a remote data center then we do not receive active
     * notification.  For this reason the settings are cached in memory for a limited amount of time before they
     * must be re-fetched from source.
     */
    private final static Duration DEFAULT_CACHE_INVALIDATION_TIME = Duration.standardMinutes(1);

    private final ValueStore<Long> _lastUpdated;
    private final Map<String, SettingImpl> _availableSettings = Maps.newConcurrentMap();
    private final LoadingCache<SettingImpl<?>, SettingValue<?>> _settingsCache;
    private final DataStore _dataStore;
    private final Supplier<String> _settingsTable;
    private final String _settingsTablePlacement;
    private final Ticker _ticker;
    private ValueStoreListener _listener;

    @Inject
    public SettingsManager(ValueStore<Long> lastUpdated, DataStore dataStore, String settingsTable,
                           String settingsTablePlacement, LifeCycleRegistry lifeCycleRegistry) {
        this(lastUpdated, dataStore, settingsTable, settingsTablePlacement, lifeCycleRegistry,
                DEFAULT_CACHE_INVALIDATION_TIME, Ticker.systemTicker());
    }

    public SettingsManager(ValueStore<Long> lastUpdated, DataStore dataStore, String settingsTable,
                           String settingsTablePlacement, LifeCycleRegistry lifeCycleRegistry,
                           Duration cacheInvalidationTime, Ticker ticker) {
        _lastUpdated = lastUpdated;
        _dataStore = dataStore;
        _settingsTablePlacement = settingsTablePlacement;
        _ticker = ticker;

        _settingsTable = Suppliers.memoize(() -> {
            // Create the settings table if it does not exist
            if (!_dataStore.getTableExists(settingsTable)) {
                _dataStore.createTable(
                        settingsTable,
                        new TableOptionsBuilder().setPlacement(_settingsTablePlacement).build(),
                        ImmutableMap.<String, Object>of(),
                        new AuditBuilder().setLocalHost().setComment("create settings table").build());
            }
            return settingsTable;
        });

        _settingsCache = CacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(cacheInvalidationTime.getMillis(), TimeUnit.MILLISECONDS)
                .build(new CacheLoader<SettingImpl<?>, SettingValue<?>>() {
                    @Override
                    public SettingValue<?> load(SettingImpl<?> setting) throws Exception {
                        Map<String, Object> valueMap = _dataStore.get(_settingsTable.get(), setting.getName(), ReadConsistency.STRONG);
                        if (Intrinsic.isDeleted(valueMap)) {
                            return SettingValue.absent();
                        }
                        String rawValue = (String) valueMap.get(VALUE_ATTRIBUTE);
                        return SettingValue.present(JsonHelper.fromJson(rawValue, setting.getTypeReference()));
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
    public <T> Setting<T> register(String name, Class<T> clazz, @Nullable T defaultValue) {
        return register(name, asTypeReference(clazz), defaultValue);
    }

    @Override
    public <T> Setting<T> register(String name, TypeReference<T> typeReference, @Nullable T defaultValue) {
        checkNotNull(name, "name");
        checkNotNull(typeReference, "typeReference");
        SettingImpl<T> newSetting = new SettingImpl<>(name, typeReference, defaultValue);
        SettingImpl<?> existingSetting = _availableSettings.putIfAbsent(name, newSetting);
        if (existingSetting != null) {
            checkState(existingSetting.metadataEquals(newSetting),
                    "Setting %s already registered with incompatible parameters", name);
            //noinspection unchecked
            return (SettingImpl<T>) existingSetting;
        }
        return newSetting;
    }

    private void set(String setting, Object value) {
        Delta delta =  Deltas.mapBuilder().put(VALUE_ATTRIBUTE, JsonHelper.asJson(value)).build();

        // Write the delta to the store
        _dataStore.update(_settingsTable.get(), setting, TimeUUIDs.newUUID(), delta,
                new AuditBuilder().setLocalHost().setComment("Updated setting").build(), WriteConsistency.STRONG);

        // Notify all nodes in the local cluster to refresh immediately; remote clusters will eventually see the update
        try {
            _lastUpdated.set(TimeUnit.NANOSECONDS.toMillis(_ticker.read()));
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
        SettingImpl<?> setting = _availableSettings.get(name);
        if (setting != null && setting.getTypeReference().getType().equals(typeReference.getType())) {
            //noinspection unchecked
            return (Setting<T>) setting;
        }
        return null;
    }

    @Override
    public Map<String, Object> getAll() {
        Map<String, Object> settings = Maps.newHashMap();
        for (SettingImpl<?> setting : _availableSettings.values()) {
            settings.put(setting.getName(), setting.get());
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

    private final class SettingImpl<T> implements Setting<T> {

        private final String _name;
        private final TypeReference<T> _typeReference;
        private final T _defaultValue;

        private SettingImpl(String name, TypeReference<T> typeReference, T defaultValue) {
            _name = name;
            _typeReference = typeReference;
            _defaultValue = defaultValue;
        }

        @Override
        public String getName() {
            return _name;
        }

        private TypeReference<T> getTypeReference() {
            return _typeReference;
        }

        @Override
        public T get() {
            //noinspection unchecked
            SettingValue<T> value = (SettingValue<T>) _settingsCache.getUnchecked(this);
            if (value.isPresent()) {
                return value.getValue();
            }
            return _defaultValue;
        }

        @Override
        public void set(@Nullable T value) {
            SettingsManager.this.set(_name, value);
        }

        /**
         * Returns true if the provided setting instance monitors the same exact setting as this instance.
         * Practically the same as {@link Object#equals(Object)} but named differently to avoid confusion
         * since this is comparing the metadata for the setting and not the setting's current value.
         */
        public boolean metadataEquals(SettingImpl<?> setting) {
            return _name.equals(setting._name) &&
                    _typeReference.getType().equals(setting._typeReference.getType()) &&
                    Objects.equal(_defaultValue, setting._defaultValue);
        }
    }

    /**
     * Class similar to Optional except supports null values.
     */
    private final static class SettingValue<T> {
        private final boolean _present;
        private final T _value;

        private static <T> SettingValue<T> absent() {
            return new SettingValue<>(false, null);
        }

        private static <T> SettingValue<T> present(@Nullable T value) {
            return new SettingValue<>(true, value);
        }

        private SettingValue(boolean present, T value) {
            _present = present;
            _value = value;
        }

        private boolean isPresent() {
            return _present;
        }

        private T getValue() {
            return _value;
        }
    }
}
