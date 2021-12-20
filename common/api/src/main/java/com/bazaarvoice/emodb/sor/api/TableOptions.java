package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class TableOptions {
    private final String _placement;
    private final List<FacadeOptions> _facades;

    TableOptions(@JsonProperty("placement") String placement, @JsonProperty("facades") List<FacadeOptions> facadeOptions) {
        _placement = requireNonNull(placement, "Table option is required: placement");
        _facades = Optional.ofNullable(facadeOptions).orElse(Collections.emptyList());
    }

    /**
     * Returns a placement string in the format "keyspace:column_family_prefix".
     */
    public String getPlacement() {
        return _placement;
    }

    /**
     * Returns facades list for this table.
     */
    public List<FacadeOptions> getFacades() {
        return _facades;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableOptions)) {
            return false;
        }
        TableOptions that = (TableOptions) o;
        return _placement.equals(that._placement) &&
                _facades.equals(that._facades);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_placement, _facades);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("placement", _placement)
                .toString();
    }
}
