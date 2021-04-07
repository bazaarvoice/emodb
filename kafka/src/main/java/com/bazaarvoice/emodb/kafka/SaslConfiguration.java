package com.bazaarvoice.emodb.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class SaslConfiguration {

    public final static String PROTOCOL = "SASL_SSL";
    public final static String SASL_MECHANISM = "SCRAM-SHA-512";
    private final String SASL_JAAS_CONFIG = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";

    @Valid
    @NotNull
    @JsonProperty("username")
    private String _saslUserName;

    @Valid
    @NotNull
    @JsonProperty("password")
    private String _saslPassword;

    public String getSaslUserName() {
        return this._saslUserName;
    }

    public String getSaslPassword() {
        return this._saslPassword;
    }

    public void set_SaslUserName(String saslUserName) {
        this._saslUserName = saslUserName;
    }

    public void set_SaslPassword(String saslPassword) {
        this._saslPassword = saslPassword;
    }

    public String getJaasConfig() {
        return String.format(SASL_JAAS_CONFIG, this._saslUserName, this._saslPassword);
    }
}
