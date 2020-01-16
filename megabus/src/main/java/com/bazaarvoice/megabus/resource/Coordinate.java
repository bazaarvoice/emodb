package com.bazaarvoice.megabus.resource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Reference to a Table/Key pair which is a part of MegabusRef.
 */
@JsonIgnoreProperties (ignoreUnknown = true)
public class Coordinate {
    private final String _table;
    private final String _key;


    public static Coordinate of(String table, String key) {
        return new Coordinate(table, key);
    }

    @JsonCreator
    public Coordinate(@JsonProperty ("table") String table, @JsonProperty ("key") String key) {
        _table = requireNonNull(table, "table");
        _key = requireNonNull(key, "key");
    }

    public String getTable() {
        return _table;
    }

    public String getKey() {
        return _key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Coordinate)) {
            return false;
        }
        Coordinate that = (Coordinate) o;
        return Objects.equal(_table, that._table) &&
                Objects.equal(_key, that._key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_table, _key);
    }

    @Override
    public String toString() {
        return _table + "/" + _key;
    }
}
