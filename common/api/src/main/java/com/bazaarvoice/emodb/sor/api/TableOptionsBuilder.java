package com.bazaarvoice.emodb.sor.api;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class TableOptionsBuilder {
    private String _placement;
    private List<FacadeOptions> _facades = Collections.emptyList();

    public TableOptionsBuilder setPlacement(String placement) {
        _placement = Objects.requireNonNull(placement, "Placement cannot be null");
        return this;
    }

    public TableOptionsBuilder setFacades(List<FacadeOptions> facades) {
        _facades = Objects.requireNonNull(facades, "Facades cannot be null");
        return this;
    }

    public TableOptions build() {
        return new TableOptions(_placement, _facades);
    }
}
