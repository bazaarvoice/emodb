package com.bazaarvoice.emodb.databus.client2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ComparisonChain;

import java.io.Serializable;
import java.util.Objects;

/**
 * Version information about an EmoDB document.
 */
public class DocumentVersion implements Serializable, Comparable<DocumentVersion> {

    private final long _version;
    private final long _lastUpdateTs;

    @JsonCreator
    public DocumentVersion(@JsonProperty("version") long version, @JsonProperty("lastUpdateTs") long lastUpdateTs) {
        _version = version;
        _lastUpdateTs = lastUpdateTs;
    }

    public long getVersion() {
        return _version;
    }

    public long getLastUpdateTs() {
        return _lastUpdateTs;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof DocumentVersion)) {
            return false;
        }

        DocumentVersion that = (DocumentVersion) o;

        return _version == that._version && _lastUpdateTs == that._lastUpdateTs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(_version, _lastUpdateTs);
    }

    @Override
    public int compareTo(DocumentVersion o) {
        return ComparisonChain.start()
                .compare(_version, o._version)
                .compare(_lastUpdateTs, o._lastUpdateTs)
                .result();
    }
}
