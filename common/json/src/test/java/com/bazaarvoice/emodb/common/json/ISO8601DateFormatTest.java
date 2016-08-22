package com.bazaarvoice.emodb.common.json;

import org.testng.annotations.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

public class ISO8601DateFormatTest {
    @Test
    public void testFormatNow() {
        Date now = new Date();

        String actual = new ISO8601DateFormat().format(now);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        String expected = sdf.format(now);

        assertEquals(actual, expected);
    }

    @Test
    public void testFormat() {
        assertEquals(new ISO8601DateFormat().format(new Date(1386372357023L)), "2013-12-06T23:25:57.023Z");
    }

    @Test
    public void testParse() throws ParseException {
        assertEquals(new ISO8601DateFormat().parse("2013-12-06T23:25:57.023Z"), new Date(1386372357023L));
    }

    @Test
    public void testClone() {
        // The format object is immutable so clone() (which is called by Jackson) should return the original object.
        DateFormat format = new ISO8601DateFormat();
        assertSame(format.clone(), format);
    }
}
