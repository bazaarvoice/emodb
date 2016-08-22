package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.fasterxml.jackson.databind.util.ISO8601Utils;
import org.joda.time.DateTime;

import java.util.Date;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wrapper around a Json Map with helper methods that facilitate type-safe access of map values.
 */
class JsonMap {
    private final Map<String, Object> _json;

    JsonMap(Map<String, Object> json) {
        _json = checkNotNull(json, "json");
    }

    Map<String, Object> getRawJson() {
        return _json;
    }

    boolean containsKey(Attribute<?> key) {
        return key.containsKey(_json);
    }

    <T> T get(Attribute<T> key) {
        return key.get(_json);
    }

    static class Attribute<T> {
        protected final String _key;

        static <T> Attribute<T> create(String key) {
            return new Attribute<>(key);
        }

        Attribute(String key) {
            _key = checkNotNull(key, "key");
        }

        String key() {
            return _key;
        }

        boolean containsKey(Map<String, ?> json) {
            return json.containsKey(_key);
        }

        T get(Map<String, ?> json) {
            // Skip runtime type checks here.  Usually the caller will verify the type with a cast anyway.
            // The main purpose of this method is to ensure at compile time that an attribute isn't used
            // in an inconsistent way--eg. as a String in some places and as a Integer elsewhere.
            //noinspection unchecked
            return (T) json.get(_key);
        }
    }

    static class TimestampAttribute extends Attribute<DateTime> {
        static Attribute<DateTime> create(String key) {
            return new TimestampAttribute(key);
        }

        private TimestampAttribute(String key) {
            super(key);
        }

        @Override
        DateTime get(Map<String, ?> json) {
            return parse((String) json.get(_key));
        }

        static String format(DateTime timestamp) {
            return timestamp != null ? ISO8601Utils.format(new Date(timestamp.getMillis()), true) : null;
        }

        static DateTime parse(String string) {
            return string != null ? new DateTime(JsonHelper.parseTimestamp(string).getTime()) : null;
        }
    }
}
