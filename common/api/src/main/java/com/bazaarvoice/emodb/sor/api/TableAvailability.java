package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class TableAvailability {
    private final String _placement;
    private final boolean _facade;

    public TableAvailability(@JsonProperty ("placement") String placement, @JsonProperty ("facade") boolean facade) {
        if (placement == null) {
            throw new NullPointerException("Table option is required: placement");
        }
        _placement = placement;
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
        return _placement.equals(that._placement) &&
                _facade == that._facade;
    }

    @Override
    public int hashCode() {
        return Objects.hash(_placement, _facade);
    }

    @Override
    public String toString() {
        return String.format("%s{placement=%s,facade=%s}", TableAvailability.class.getName(), _placement, _facade);
    }
}
