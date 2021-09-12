package com.bazaarvoice.emodb.blob.db.s3.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class S3HealthCheckConfiguration {

    private final static String DEFAULT_NAME = "blob-s3";
    private final static Duration DEFAULT_DURATION = Duration.of(60, ChronoUnit.SECONDS);
    /**
     * Name for the health check as registered in DropWizard.
     */

    @JsonProperty("name")
    private String _name = DEFAULT_NAME;


    @JsonProperty("duration")
    private String _duration = DEFAULT_DURATION.toString();


    public String getName() {
        return _name;
    }

    public Duration getDuration() {
        return Duration.parse(_duration);
    }

}
