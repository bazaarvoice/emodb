package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class UnknownFacadeException extends RuntimeException {
    private final String _facade;

    public UnknownFacadeException() {
        _facade = null;
    }

    public UnknownFacadeException(String facade) {
        super(facade);
        _facade = facade;
    }

    @JsonCreator
    public UnknownFacadeException(@JsonProperty ("message") String message, @JsonProperty ("facade") String facade) {
        super(message);
        _facade = facade;
    }

    public UnknownFacadeException(String message, Throwable cause) {
        super(message, cause);
        _facade = null;
    }

    public String getFacade() {
        return _facade;
    }
}
