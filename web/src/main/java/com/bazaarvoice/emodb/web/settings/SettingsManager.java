package com.bazaarvoice.emodb.web.settings;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Implementations of {@link SettingsRegistry} and {@link Settings} that is backed by a system table.  Each
 * registered setting is stored as a row in the table.  Values are cached for a set time (one minute by default)
 * so reading the values from the returned settings typically do no incur a round trip to Cassandra.
 */
public class SettingsManager implements SettingsRegistry, Settings {

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

    private final Map<String, RegisteredSetting<?>> _registeredSettings = new ConcurrentHashMap<>();
    private final LoadingCache<String, Object> _settingsCache;
    private final Supplier<DataStore> _dataStore;
    private final Supplier<String> _settingsTable;
    private final String _settingsTablePlacement;
    private final CacheHandle _cacheHandle;

    /**
     * Injection constructor.  Because of circular dependency issues between the SettingsRegistry interface and the
     * DataStore the latter is injected using its provider.
     */
    @Inject
    public SettingsManager(Provider<DataStore> dataStoreProvider, String settingsTable,
                           String settingsTablePlacement, @SettingsCacheRegistry CacheRegistry settingsCacheRegistry) {

        _dataStore = Suppliers.memoize(dataStoreProvider::get);
        _settingsTablePlacement = settingsTablePlacement;

        _settingsTable = Suppliers.memoize(() -> {
            // Create the settings table if it does not exist
            if (!_dataStore.get().getTableExists(settingsTable)) {
                _dataStore.get().createTable(
                        settingsTable,
                        new TableOptionsBuilder().setPlacement(_settingsTablePlacement).build(),
                        Collections.EMPTY_MAP,
                        new AuditBuilder().setLocalHost().setComment("create settings table").build());
            }
            return settingsTable;
        });

        _settingsCache = CacheBuilder.newBuilder()
                .build(new CacheLoader<String, Object>() {
                    @Override
                    public Object load(String name) throws Exception {
                        RegisteredSetting<?> registeredSetting = _registeredSettings.get(name);
                        Objects.requireNonNull(registeredSetting, "Cache value lookup for unregistered setting: " + name);
                        SettingMetadata<?> metadata = registeredSetting.metadata;

                        Map<String, Object> valueMap = _dataStore.get().get(
                                _settingsTable.get(), metadata.getName(), ReadConsistency.STRONG);

                        if (Intrinsic.isDeleted(valueMap)) {
                            return metadata.getDefaultValue();
                        }
                        Object value;
                        int version = Optional.ofNullable((Integer) valueMap.get(VERSION_ATTRIBUTE)).orElse(-1);
                        if (version == CURRENT_SETTING_VERSION) {
                            String rawValue = (String) valueMap.get(VALUE_ATTRIBUTE);
                            value = JsonHelper.fromJson(rawValue, metadata.getTypeReference());
                        } else {
                            throw new IllegalStateException("Setting stored with unparseable version: " + version);
                        }

                        _log.info("Setting {} updated: {}", name, value);
                        return value;
                    }
                });

        _cacheHandle = settingsCacheRegistry.register("settings", _settingsCache, true);
    }

    @Override
    public <T> Setting<T> register(String name, Class<T> clazz, T defaultValue) {
        return register(name, asTypeReference(clazz), defaultValue);
    }

    @Override
    public <T> Setting<T> register(String name, TypeReference<T> typeReference, T defaultValue) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(typeReference, "typeReference");
        Objects.requireNonNull(defaultValue, "defaultValue");

        SettingMetadata<T> metadata = new SettingMetadata<>(name, typeReference, defaultValue);
        Setting<T> setting = createSetting(metadata);
        RegisteredSetting newSetting = new RegisteredSetting<>(metadata, setting);

        RegisteredSetting<?> registered = _registeredSettings.putIfAbsent(name, newSetting);
        if (registered != null) {
            if (!registered.metadata.equals(metadata)) {
                throw new IllegalStateException("Setting " + name + " already registered with incompatible parameters");
            }
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
        return (T) _settingsCache.getUnchecked(metadata.getName());
    }

    private <T> void set(SettingMetadata<T> metadata, T value) {
        Objects.requireNonNull(value, "value");

        Delta delta = Deltas.mapBuilder()
                .put(VALUE_ATTRIBUTE, JsonHelper.asJson(value))
                .put(VERSION_ATTRIBUTE, CURRENT_SETTING_VERSION)
                .build();

        // Write the delta to the store
        _dataStore.get().update(_settingsTable.get(), metadata.getName(), TimeUUIDs.newUUID(), delta,
                new AuditBuilder().setLocalHost().setComment("Updated setting").build(), WriteConsistency.GLOBAL);

        // Notify all instances that the setting value has changed
        _cacheHandle.invalidate(InvalidationScope.GLOBAL, metadata.getName());
    }

    @Override
    public <T> Setting<T> getSetting(String name, Class<T> clazz) {
        return getSetting(name, asTypeReference(clazz));
    }

    @Override
    public <T> Setting<T> getSetting(String name, TypeReference<T> typeReference) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(typeReference, "typeReference");

        RegisteredSetting<?> registered = _registeredSettings.get(name);
        if (registered != null && registered.metadata.getTypeReference().getType().equals(typeReference.getType())) {
            //noinspection unchecked
            return (Setting<T>) registered.setting;
        }
        return null;
    }

    @Override
    public Map<String, Object> getAll() {
        Map<String, Object> settings = new HashMap<>();
        for (RegisteredSetting<?> registered : _registeredSettings.values()) {
            settings.put(registered.metadata.getName(), registered.setting.get());
        }
        return settings;
    }

    private <T> TypeReference<T> asTypeReference(Class<T> clazz) {
        Objects.requireNonNull(clazz, "class");
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
