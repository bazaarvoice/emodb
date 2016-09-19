package com.bazaarvoice.emodb.web.settings;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * POJO for metadata about a setting, namely:
 *
 * <ol>
 *     <li>name</li>
 *     <li>data type</li>
 *     <li>default value</li>
 * </ol>
 */
public class SettingMetadata<T> {

    private final String _name;
    private final TypeReference<T> _typeReference;
    private final T _defaultValue;

    public SettingMetadata(String name, TypeReference<T> typeReference, T defaultValue) {
        _name = checkNotNull(name);
        _typeReference = checkNotNull(typeReference, "type");
        _defaultValue = checkNotNull(defaultValue, "defaultValue");
    }

    public String getName() {
        return _name;
    }

    public TypeReference<T> getTypeReference() {
        return _typeReference;
    }

    public T getDefaultValue() {
        return _defaultValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SettingMetadata)) {
            return false;
        }

        SettingMetadata that = (SettingMetadata) o;

        return _name.equals(that.getName()) &&
                _typeReference.getType().equals(that.getTypeReference().getType()) &&
                _defaultValue.equals(that.getDefaultValue());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_name, _defaultValue);
    }
}
