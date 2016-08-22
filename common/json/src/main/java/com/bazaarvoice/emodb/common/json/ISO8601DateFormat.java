package com.bazaarvoice.emodb.common.json;

import com.fasterxml.jackson.databind.util.ISO8601Utils;

import java.text.FieldPosition;
import java.util.Date;

/**
 * ISO-8601 timestamp formatter and parser that's equivalent to but faster than
 * {@code new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").setTimeZone("GMT")}.
 */
public class ISO8601DateFormat extends com.fasterxml.jackson.databind.util.ISO8601DateFormat {
    // Overrides the superclass method to add milliseconds.
    @Override
    public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
        return toAppendTo.append(ISO8601Utils.format(date, true));
    }
}
