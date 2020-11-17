package com.bazaarvoice.emodb.sor.api;

import java.util.Objects;

public final class FacadeOptionsBuilder {
    private String _placement;

    public FacadeOptionsBuilder setPlacement(String placement) {
        _placement = Objects.requireNonNull(placement, "placement");
        return this;
    }

    public FacadeOptions build() {
        return new FacadeOptions(_placement);
    }
}
