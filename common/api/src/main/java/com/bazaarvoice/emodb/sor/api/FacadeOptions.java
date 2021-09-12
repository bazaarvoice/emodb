package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public final class FacadeOptions {
    private final String _placement;

    public FacadeOptions(@JsonProperty ("placement") String placement) {
        _placement = checkNotNull(placement, "Facade option is required: placement");
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
        return Objects.equal(_placement, that._placement);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_placement);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("placement", _placement)
                .toString();
    }
}
