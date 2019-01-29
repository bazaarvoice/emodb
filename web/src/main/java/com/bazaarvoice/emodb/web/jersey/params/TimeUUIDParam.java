package com.bazaarvoice.emodb.web.jersey.params;

import io.dropwizard.jersey.params.AbstractParam;

import java.util.UUID;

public class TimeUUIDParam extends AbstractParam<UUID> {

    public TimeUUIDParam(String input) {
        super(input);
    }

    @Override
    protected String errorMessage(Exception e) {
        return "Invalid uuid parameter (must be a RFC 4122 version 1 time-based uuid)";
    }

    @Override
    protected UUID parse(String input) throws Exception {
        UUID uuid = UUID.fromString(input);
        if (uuid.version() != 1) {
            throw new IllegalArgumentException(); // must be a time uuid
        }
        return uuid;
    }
}
