package com.bazaarvoice.emodb.web.jersey.params;

import com.google.common.base.CharMatcher;
import io.dropwizard.jersey.params.AbstractParam;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.concurrent.TimeUnit;

/**
 * Parses an Instant in any of the following formats:
 *
 * <ul>
 *     <li>ISO-8601.  For example, "2010-01-01T12:00:00Z" or ""2010-01-01T12:00:00+00:00"</li>
 *     <li>ISO-8601 with RFC-822 formatted zone offset.  For example, "2010-01-01T12:00:00+0000"</li>
 *     <li>ISO-8601 with no zone offset, presumed UTC.  For example, "2010-01-01T12:00:00"</li>
 *     <li>Epoch milliseconds.  For example, "1262347200000"</li>
 *     <li>Epoch nanoseconds.  For example, "126234720000000000000".  Long inputs after year 10,000 CE are presumed to be nanos.</li>
 * </ul>.
 */
public class InstantParam extends AbstractParam<Instant> {

    private static final DateTimeFormatter RFC_822_FORMAT = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .optionalStart()
            .appendOffset("+HHmm", "+0000")
            .toFormatter();

    public InstantParam(String input) {
        super(input);
    }

    @Override
    protected String errorMessage(String input, Exception e) {
        return "Invalid ISO8601 timestamp parameter: " + input;
    }

    @Override
    protected Instant parse(String input) throws Exception {
        if (CharMatcher.DIGIT.matchesAllOf(input)) {
            // Assume millis, but check for nanos
            long millis = Long.parseLong(input);
            if (millis >= 253402300800000L) { // +10000-01-01T00:00:00Z
                millis = TimeUnit.NANOSECONDS.toMillis(millis);
            }
            return Instant.ofEpochMilli(millis);
        }
        try {
            return ZonedDateTime.parse(input).toInstant();
        } catch (DateTimeParseException e) {
            TemporalAccessor ta = RFC_822_FORMAT.parse(input);
            LocalDateTime dateTime = LocalDateTime.from(ta);
            return ZonedDateTime.of(dateTime, ta.isSupported(ChronoField.OFFSET_SECONDS) ? ZoneId.from(ta) : ZoneOffset.UTC).toInstant();
        }
    }
}
