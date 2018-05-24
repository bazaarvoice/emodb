package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Metadata that is stored along with every write to help trace the source
 * and intent of each delta.  Audit information is never modified or compacted.
 */
public final class Audit {

    // predefined keys.  clients can add others at will.
    public static final String SHA1 = "~sha1";
    public static final String COMMENT = "comment";
    public static final String PROGRAM = "program";
    public static final String HOST = "host";
    public static final String USER = "user";
    public static final String TAGS = "~tags";

    private final Map<String, ?> _fields;

    @JsonCreator
    public Audit(Map<String, ?> fields) {
        if (fields == null) {
            throw new NullPointerException("Fields cannot be null");
        }
        _fields = fields;
    }

    @Nullable
    public String getComment() {
        return asString(_fields.get(COMMENT));
    }

    @Nullable
    public String getProgram() {
        return asString(_fields.get(PROGRAM));
    }

    @Nullable
    public String getHost() {
        return asString(_fields.get(HOST));
    }

    @Nullable
    public String getUser() {
        return asString(_fields.get(USER));
    }

    public List<String> getTags() {
        Object tags = _fields.get(TAGS);
        if (tags != null && tags instanceof Collection) {
            return ((Collection<?>)tags).stream()
                    .map(Object::toString)
                    .sorted()
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Nullable
    public Object getCustom(String key) {
        return _fields.get(key);
    }

    @JsonValue
    public Map<String, Object> getAll() {
        return Collections.unmodifiableMap(_fields);
    }

    private static String asString(Object value) {
        return (value != null) ? value.toString() : null;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof Audit && _fields.equals(((Audit) o).getAll());
    }

    @Override
    public int hashCode() {
        return _fields.hashCode();
    }

    @Override
    public String toString() {
        return "Audit[" + _fields + "]";
    }
}
