package com.bazaarvoice.emodb.web.auth.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotNull;


public class AWSConfig {
    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @JsonProperty
    @NotNull
    private String profile;

    @JsonProperty
    @NotNull
    private String region;
}
