package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class FacadeOptions {
    private final String _placement;

    public FacadeOptions(@JsonProperty ("placement") String placement) {
        _placement = Objects.requireNonNull(placement, "Facade option is required: placement");
    }

    /**
     * Returns a placement string in the format "keyspace:column_family_prefix".
     */
    public String getPlacement() {
        return _placement;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FacadeOptions)) {
            return false;
        }
        FacadeOptions that = (FacadeOptions) o;
        return _placement.equals(that._placement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_placement);
    }

    @Override
    public String toString() {
        return String.format("%s{placement=%s}", FacadeOptions.class.getName(), _placement);
    }
}
