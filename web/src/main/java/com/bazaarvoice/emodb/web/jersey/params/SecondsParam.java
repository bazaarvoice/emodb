package com.bazaarvoice.emodb.web.jersey.params;

import io.dropwizard.jersey.params.AbstractParam;

import java.time.Duration;

public class SecondsParam extends AbstractParam<Duration> {

    public SecondsParam(String input) {
        super(input);
    }

    @Override
    protected String errorMessage(String input, Exception e) {
        return '"' + input + "\" is not a number.";
    }

    @Override
    protected Duration parse(String input) {
        return Duration.ofSeconds(Integer.valueOf(input));
    }
}
