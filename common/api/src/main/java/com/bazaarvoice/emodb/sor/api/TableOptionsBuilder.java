package com.bazaarvoice.emodb.sor.api;

import java.util.Collections;
import java.util.List;

public final class TableOptionsBuilder {
    private String _placement;
    private List<FacadeOptions> _facades = Collections.emptyList();

    public TableOptionsBuilder setPlacement(String placement) {
        if (placement == null) {
            throw new NullPointerException("Placement cannot be null");
        }
        _placement = placement;
        return this;
    }

    public TableOptionsBuilder setFacades(List<FacadeOptions> facades) {
        if (facades == null) {
            throw new NullPointerException("Facades cannot be null");
        }
        _facades = facades;
        return this;
    }

    public TableOptions build() {
        return new TableOptions(_placement, _facades);
    }
}
