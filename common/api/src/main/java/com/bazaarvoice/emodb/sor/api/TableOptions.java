package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class TableOptions {
    private final String _placement;
    private final List<FacadeOptions> _facades;

    TableOptions(@JsonProperty("placement") String placement, @JsonProperty("facades") List<FacadeOptions> facadeOptions) {
        _placement = Objects.requireNonNull(placement, "Table option is required: placement");
        _facades = Optional.ofNullable(facadeOptions).orElse(Collections.<FacadeOptions>emptyList());
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
        return Objects.hash(_placement, _facades);
    }

    @Override
    public String toString() {
        return String.format("%s{placement=%s}", TableOptions.class.getName(), _placement);
    }
}
