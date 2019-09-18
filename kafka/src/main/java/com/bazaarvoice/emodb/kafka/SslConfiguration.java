package com.bazaarvoice.emodb.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class SslConfiguration {

    public final static String PROTOCOL = "SSL";

    @Valid
    @NotNull
    @JsonProperty("trustStoreLocation")
    private String _trustStoreLocation;

    @Valid
    @NotNull
    @JsonProperty("trustStorePassword")
    private String _trustStorePassword;

    @Valid
    @NotNull
    @JsonProperty("keyStoreLocation")
    private String _keyStoreLocation;

    @Valid
    @NotNull
    @JsonProperty("keyStorePassword")
    private String _keyStorePassword;

    @Valid
    @NotNull
    @JsonProperty("keyPassword")
    private String _keyPassword;

    public String getTrustStoreLocation() {
        return _trustStoreLocation;
    }

    public void setTrustStoreLocation(String trustStoreLocation) {
        this._trustStoreLocation = trustStoreLocation;
    }

    public String getTrustStorePassword() {
        return _trustStorePassword;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this._trustStorePassword = trustStorePassword;
    }

    public String getKeyStoreLocation() {
        return _keyStoreLocation;
    }

    public void setKeyStoreLocation(String keyStoreLocation) {
        this._keyStoreLocation = keyStoreLocation;
    }

    public String getKeyStorePassword() {
        return _keyStorePassword;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this._keyStorePassword = keyStorePassword;
    }

    public String getKeyPassword() {
        return _keyPassword;
    }

    public void setKeyPassword(String keyPassword) {
        this._keyPassword = keyPassword;
    }
}
