package com.bazaarvoice.emodb.sor.api;

import static com.google.common.base.Preconditions.checkNotNull;

public final class FacadeOptionsBuilder {
    private String _placement;

    public FacadeOptionsBuilder setPlacement(String placement) {
        _placement = checkNotNull(placement, "placement");
        return this;
    }

    public FacadeOptions build() {
        return new FacadeOptions(_placement);
    }
}
