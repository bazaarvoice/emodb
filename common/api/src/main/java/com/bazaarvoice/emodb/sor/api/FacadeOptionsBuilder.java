package com.bazaarvoice.emodb.sor.api;

public final class FacadeOptionsBuilder {
    private String _placement;

    public FacadeOptionsBuilder setPlacement(String placement) {
        if (placement == null) {
            throw new NullPointerException("placement");
        }
        _placement = placement;
        return this;
    }

    public FacadeOptions build() {
        return new FacadeOptions(_placement);
    }
}
