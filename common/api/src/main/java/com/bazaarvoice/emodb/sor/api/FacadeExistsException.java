package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties ({"cause", "localizedMessage", "stackTrace"})
public class FacadeExistsException extends RuntimeException {
    private final String _table;
    private final String _placement;

    @JsonCreator
    public FacadeExistsException(@JsonProperty ("message") String message,
                                 @JsonProperty ("table") String table,
                                 @JsonProperty ("placement") String placement) {
        super(message);
        _table = table;
        _placement = placement;
    }

    public String getTable() {
        return _table;
    }

    public String getPlacement() {
        return _placement;
    }
}
