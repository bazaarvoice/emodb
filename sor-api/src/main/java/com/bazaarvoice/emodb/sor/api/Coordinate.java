package com.bazaarvoice.emodb.sor.api;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

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
        requireNonNull(string, "string");
        int delim = string.indexOf('/');
        if (delim == -1) {
            throw new IllegalArgumentException("Invalid coordinate format.");
        }
        String table = string.substring(0, delim);
        String id = string.substring(delim + 1);
        if (!com.bazaarvoice.emodb.common.api.Names.isLegalTableName(table)) {
            throw new IllegalArgumentException("Invalid coordinate format: invalid table name");
        }
        if (id.isEmpty()) {
            throw new IllegalArgumentException("Invalid coordinate format: missing identifier");
        }
        return new Coordinate(table, id);
    }

    private Coordinate(String table, String id) {
        _table = requireNonNull(table, "table");
        _id = requireNonNull(id, "id");
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
    public Map<String, Object> asJson() {
        Map<String, Object> map = new HashMap<>();
        map.put(Intrinsic.TABLE, _table);
        map.put(Intrinsic.ID, _id);
        return map;
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
