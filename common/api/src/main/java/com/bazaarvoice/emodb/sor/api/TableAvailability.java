package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.util.Objects;

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

public final class TableAvailability {
    private final String _placement;
    private final boolean _facade;

    public TableAvailability(@JsonProperty ("placement") String placement, @JsonProperty ("facade") boolean facade) {
        _placement = requireNonNull(placement, "Table option is required: placement");
        _facade = facade;
    }

    /**
     * Returns a placement string in the format "keyspace:column_family_prefix".
     */
    public String getPlacement() {
        return _placement;
    }

    /**
     * Returns if this is a facade
     */
    public boolean isFacade() {
        return _facade;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableAvailability)) {
            return false;
        }
        TableAvailability that = (TableAvailability) o;
        return Objects.equals(_placement, that._placement) &&
                Objects.equals(_facade, that._facade);
    }

    @Override
    public int hashCode() {
        return hash(_placement, _facade);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("placement", _placement)
                .add("facade", _facade)
                .toString();
    }
}
