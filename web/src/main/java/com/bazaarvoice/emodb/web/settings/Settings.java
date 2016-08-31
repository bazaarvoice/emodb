package com.bazaarvoice.emodb.web.settings;

import com.fasterxml.jackson.core.type.TypeReference;

import java.util.Map;

/**
 * General interface for reading settings.  In order to create new settings use one of the <code>register</code>
 * methods from {@link SettingsRegistry}, such as {@link SettingsRegistry#register(String, Class, Object)}.
 *
 * Typically the setting returned by the registry can be used throughout the system.  This interface is provided
 * primarily for debugging purposes, such as dumping the values of all settings.
 */
public interface Settings {

    /**
     * Returns a setting previously registered using {@link SettingsRegistry#register(String, Class, Object)}
     * with the same name and type.
     *
     * @param name The setting name
     * @param clazz The setting's value type
     * @param <T> The setting's value type
     * @return The setting, or null if no matching setting was found
     */
    <T> Setting<T> getSetting(String name, Class<T> clazz);

    /**
     * Returns a setting previously registered using {@link SettingsRegistry#register(String, TypeReference, Object)}
     * with the same name and type.
     *
     * @param name The setting name
     * @param typeReference The setting's value type
     * @param <T> The setting's value type
     * @return The setting, or null if no matching setting was found
     */
    <T> Setting<T> getSetting(String name, TypeReference<T> typeReference);

    /**
     * Gets a copy of all registered settings' current values.  This map is a snapshot and is not backed by the actual
     * settings.  Updates to the settings will not be reflected in the map and updating the map will not change
     * any settings.
     *
     * @return A copy of the all registered setting's current values.
     */
    Map<String, Object> getAll();
}
