package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An EmoDB coordinate consisting of the pair of a table name and a string identifier.
 */
public final class Coordinate {
    private final String _table;
    private final String _id;

    public static Coordinate of(String table, String id) {
        return new Coordinate(table, id);
    }

    /**
     * Creates a coordinate from a Json map with with "~table" and "~id".  This is the inverse of {@link #asJson()}.
     */
    public static Coordinate fromJson(Map<String, ?> json) {
        return Coordinate.of(Intrinsic.getTable(json), Intrinsic.getId(json));
    }

    /**
     * Parses a string in the format "&lt;table>/&lt;id>".  This is the inverse of {@link #toString()}.
     */
    public static Coordinate parse(String string) {
        checkNotNull(string, "string");
        int delim = string.indexOf('/');
        checkArgument(delim != -1, "Invalid coordinate format.");
        String table = string.substring(0, delim);
        String id = string.substring(delim + 1);
        checkArgument(com.bazaarvoice.emodb.common.api.Names.isLegalTableName(table), "Invalid coordinate format: invalid table name");
        checkArgument(!id.isEmpty(), "Invalid coordinate format: missing identifier");
        return new Coordinate(table, id);
    }

    @JsonCreator
    private Coordinate(@JsonProperty(Intrinsic.TABLE) String table, @JsonProperty(Intrinsic.ID) String id) {
        _table = checkNotNull(table, "table");
        _id = checkNotNull(id, "id");
    }

    public String getTable() {
        return _table;
    }

    public String getId() {
        return _id;
    }

    /**
     * Returns a Json map with two entries, one for "~table" and one for "~id", similar to all System of Record objects.
     */
    @JsonIgnore
    public Map<String, Object> asJson() {
        return ImmutableMap.<String, Object>of(
                Intrinsic.TABLE, _table,
                Intrinsic.ID, _id);
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
        return _id.equals(that.getId()) && _table.equals(that.getTable());
    }

    @Override
    public int hashCode() {
        return 31 * _table.hashCode() + _id.hashCode();
    }

    @Override
    public String toString() {
        return _table + "/" + _id;
    }
}
