package com.bazaarvoice.emodb.web.jersey.params;

import io.dropwizard.jersey.params.AbstractParam;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Date;

/**
 * Parses a Date object in ISO8601 format.  For example, "2010-01-01T12:00:00Z".
 */
public class TimestampParam extends AbstractParam<Date> {

    public TimestampParam(String input) {
        super(input);
    }

    @Override
    protected String errorMessage(String input, Exception e) {
        return "Invalid ISO8601 timestamp parameter: " + input;
    }

    @Override
    protected Date parse(String input) throws Exception {
        // The Jackson timestamp parser is relatively forgiving, accepts a number of format variations.
        return ISODateTimeFormat.dateTimeParser().parseDateTime(input).toDate();
    }
}
