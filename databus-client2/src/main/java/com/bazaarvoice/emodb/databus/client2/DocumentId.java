package com.bazaarvoice.emodb.databus.client2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ComparisonChain;

import java.io.Serializable;
import java.util.Objects;

/**
 * Unique identifier for a document in EmoDB.
 */
public class DocumentId implements Serializable, Comparable<DocumentId> {

    private final String _table;
    private final String _key;

    @JsonCreator
    public DocumentId(@JsonProperty("table") String table, @JsonProperty("key") String key) {
        _table = table;
        _key = key;
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
        if (!(o instanceof DocumentId)) {
            return false;
        }

        DocumentId that = (DocumentId) o;

        return Objects.equals(_table, that._table) && Objects.equals(_key, that._key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_table, _key);
    }

    @Override
    public int compareTo(DocumentId o) {
        return ComparisonChain.start()
                .compare(_table, o._table)
                .compare(_key, o._key)
                .result();
    }

    @Override
    public String toString() {
        return String.format("%s/%s", _table, _key);
    }
}
