package com.bazaarvoice.emodb.sor.api;

import static java.util.Objects.requireNonNull;

public final class FacadeOptionsBuilder {
    private String _placement;

    public FacadeOptionsBuilder setPlacement(String placement) {
        _placement = requireNonNull(placement, "placement");
        return this;
    }

    public FacadeOptions build() {
        return new FacadeOptions(_placement);
    }
}
