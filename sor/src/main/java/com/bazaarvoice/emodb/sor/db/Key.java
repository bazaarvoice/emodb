package com.bazaarvoice.emodb.sor.db;

import com.bazaarvoice.emodb.table.db.Table;

import static com.google.common.base.Preconditions.checkNotNull;

public class Key {
    private final Table _table;
    private final String _key;

    public Key(Table table, String key) {
        _table = checkNotNull(table, "table");
        _key = checkNotNull(key, "key");
    }

    public Table getTable() {
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
        if (!(o instanceof Key)) {
            return false;
        }
        Key key = (Key) o;
        return _key.equals(key._key) && _table.equals(key._table);
    }

    @Override
    public int hashCode() {
        return 31 * _table.hashCode() + _key.hashCode();
    }

    @Override
    public String toString() {
        return _table.getName() + "/" + _key; // for debugging
    }
}
