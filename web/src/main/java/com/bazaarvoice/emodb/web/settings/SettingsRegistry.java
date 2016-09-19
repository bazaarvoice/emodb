package com.bazaarvoice.emodb.web.settings;

import com.fasterxml.jackson.core.type.TypeReference;

import javax.annotation.Nullable;

/**
 * Registers new settings for use throughout the system.  The actual behavior of each setting, such as
 * value caching and propagation delay on update, is up to the implementation.
 */
public interface SettingsRegistry {

    /**
     * Registers a setting.  If the setting is already registered with the same parameters then the existing setting
     * is returned.  If a setting is already registered with the same name but different type or default value
     * then an exception is thrown.
     *
     * @param name The setting name
     * @param clazz The setting type
     * @param defaultValue The default value for the setting, or null if null should be returned if no value is set
     * @param <T> The setting type
     * @return The setting
     */
    <T> Setting<T> register(String name, Class<T> clazz, @Nullable T defaultValue);

    /**
     * Identical behavior to {@link #register(String, Class, Object)} with a type parameter that allows for generic
     * types, such as <code>List&lt;String&gt;</code>.
     *
     * @see #register(String, Class, Object)
     */
    <T> Setting<T> register(String name, TypeReference<T> type, @Nullable T defaultValue);
}
