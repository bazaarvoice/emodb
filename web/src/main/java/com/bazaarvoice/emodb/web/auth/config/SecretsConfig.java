package com.bazaarvoice.emodb.web.auth.config;

import com.fasterxml.jackson.annotation.JsonProperty;


import javax.validation.constraints.NotNull;


public class SecretsConfig {

    @NotNull
    @JsonProperty
    private String secretName;

    public String getSecretName() {
        return secretName;
    }

    public void setSecretName(String secretName) {
        this.secretName = secretName;
    }

    public String getSecretId() {
        return secretId;
    }

    public void setSecretId(String secretId) {
        this.secretId = secretId;
    }

    @JsonProperty
    private String secretId;

}
