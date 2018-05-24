package com.bazaarvoice.emodb.web.jersey.params;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Instant;

import static org.testng.Assert.assertEquals;

public class InstantParamTest {

    @DataProvider(name = "inputs")
    public Object[][] getInputs() {
        return new Object[][] {
                // Instant notation, ZoneOffset "Z"
                new Object[] { "2018-01-15T12:30:20.123Z", Instant.parse("2018-01-15T12:30:20.123Z") },
                new Object[] { "2018-01-15T12:30:20Z", Instant.parse("2018-01-15T12:30:20.000Z") },
                new Object[] { "2018-01-15T12:30Z", Instant.parse("2018-01-15T12:30:00.000Z") },
                // Instant notation, ISO-8601 UTC
                new Object[] { "2018-01-15T12:30:20.123+00:00", Instant.parse("2018-01-15T12:30:20.123Z") },
                new Object[] { "2018-01-15T12:30:20+00:00", Instant.parse("2018-01-15T12:30:20.000Z") },
                new Object[] { "2018-01-15T12:30+00:00", Instant.parse("2018-01-15T12:30:00.000Z") },
                // Instant notation, ISO-8601 zone
                new Object[] { "2018-01-15T12:30:20.123+05:00", Instant.parse("2018-01-15T07:30:20.123Z") },
                new Object[] { "2018-01-15T12:30:20+05:00", Instant.parse("2018-01-15T07:30:20.000Z") },
                new Object[] { "2018-01-15T12:30+05:00", Instant.parse("2018-01-15T07:30:00.000Z") },
                // Instant notation, RFC-232 UTC
                new Object[] { "2018-01-15T12:30:20.123+0000", Instant.parse("2018-01-15T12:30:20.123Z") },
                new Object[] { "2018-01-15T12:30:20+0000", Instant.parse("2018-01-15T12:30:20.000Z") },
                new Object[] { "2018-01-15T12:30+0000", Instant.parse("2018-01-15T12:30:00.000Z") },
                // Instant notation, RFC-232 zone
                new Object[] { "2018-01-15T12:30:20.123+0600", Instant.parse("2018-01-15T06:30:20.123Z") },
                new Object[] { "2018-01-15T12:30:20+0600", Instant.parse("2018-01-15T06:30:20.000Z") },
                new Object[] { "2018-01-15T12:30+0600", Instant.parse("2018-01-15T06:30:00.000Z") },
                // Instant notation, no zone (UTC implied)
                new Object[] { "2018-01-15T12:30:20.123", Instant.parse("2018-01-15T12:30:20.123Z") },
                new Object[] { "2018-01-15T12:30:20", Instant.parse("2018-01-15T12:30:20.000Z") },
                new Object[] { "2018-01-15T12:30", Instant.parse("2018-01-15T12:30:00.000Z") },
                // Epoch millis
                new Object[] { "1516019420123", Instant.parse("2018-01-15T12:30:20.123Z") },
                new Object[] { "1516019420000", Instant.parse("2018-01-15T12:30:20.000Z") },
                new Object[] { "1516019400000", Instant.parse("2018-01-15T12:30:00.000Z") },
                // Epoch nanos
                new Object[] { "1516019420123000000", Instant.parse("2018-01-15T12:30:20.123Z") },
                new Object[] { "1516019420000000000", Instant.parse("2018-01-15T12:30:20.000Z") },
                new Object[] { "1516019400000000000", Instant.parse("2018-01-15T12:30:00.000Z") }
        };
    }

    @Test(dataProvider = "inputs")
    public void testInstantParamParser(String input, Instant expected) {
        assertEquals(new InstantParam(input).get(), expected);
    }
}
