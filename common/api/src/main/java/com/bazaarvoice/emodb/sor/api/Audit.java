package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Functions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

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
        _fields = checkNotNull(fields, "fields");
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
            return Ordering.natural().sortedCopy(FluentIterable.from((Collection<?>) tags).transform(Functions.toStringFunction())
                    .toList());
        }
        return ImmutableList.of();
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
