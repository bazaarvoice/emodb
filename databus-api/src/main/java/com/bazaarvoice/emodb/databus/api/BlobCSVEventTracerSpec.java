package com.bazaarvoice.emodb.databus.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.joda.time.Duration;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link DatabusEventTracerSpec} implementation which writes the event trace to a CSV file, which is then persisted in
 * the blob store.  By default documents are written gzipped with a 14-day TTL, although both of these can be
 * overridden in the spec.
 *
 * Columns in the resulting CSV are, by position:
 *
 * <ol>
 *     <li>Original document change ID</li>
 *     <li>Document change timestamp</li>
 *     <li>table</li>
 *     <li>key</li>
 *     <li>trace comment, typically debug information about how the change was included in the requested operation</li>
 * </ol>
 */
@JsonTypeName("blob")
public class BlobCSVEventTracerSpec implements DatabusEventTracerSpec {

    private final String _table;
    private final String _blobId;
    private boolean _gzipped = true;
    private Duration _ttl = Duration.standardDays(14);

    @JsonCreator
    public BlobCSVEventTracerSpec(@JsonProperty("table") String table, @JsonProperty("blobId") String blobId) {
        _table = checkNotNull(table, "table");
        _blobId = checkNotNull(blobId, "blobId");
    }

    public String getTable() {
        return _table;
    }

    public String getBlobId() {
        return _blobId;
    }

    public boolean isGzipped() {
        return _gzipped;
    }

    @JsonProperty("gzipped")
    public BlobCSVEventTracerSpec gzipped(boolean gzipped) {
        _gzipped = gzipped;
        return this;
    }

    public Duration getTtl() {
        return _ttl;
    }

    @JsonProperty("ttl")
    public BlobCSVEventTracerSpec ttl(Duration ttl) {
        _ttl = ttl;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BlobCSVEventTracerSpec)) {
            return false;
        }

        BlobCSVEventTracerSpec that = (BlobCSVEventTracerSpec) o;

        return _gzipped == that.isGzipped() &&
                _table.equals(that.getTable()) &&
                _blobId.equals(that.getBlobId()) &&
                Objects.equals(_ttl, that.getTtl());
    }

    @Override
    public int hashCode() {
        return Objects.hash(_table, _blobId, _gzipped, _ttl);
    }
}