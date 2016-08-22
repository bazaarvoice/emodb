package com.bazaarvoice.emodb.sor.api;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public final class TableOptionsBuilder {
    private String _placement;
    private List<FacadeOptions> _facades = Collections.emptyList();

    public TableOptionsBuilder setPlacement(String placement) {
        _placement = checkNotNull(placement, "placement");
        return this;
    }

    public TableOptionsBuilder setFacades(List<FacadeOptions> facades) {
        _facades = checkNotNull(facades, "facades");
        return this;
    }

    public TableOptions build() {
        return new TableOptions(_placement, _facades);
    }
}
