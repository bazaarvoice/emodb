package com.bazaarvoice.emodb.common.json;

import org.testng.annotations.Test;

import java.text.ParseException;
import java.util.Date;

import static org.testng.Assert.assertEquals;

public class JsonHelperTest {
    @Test
    public void testFormat() {
        assertEquals(JsonHelper.formatTimestamp(new Date(1386372357023L)), "2013-12-06T23:25:57.023Z");
        assertEquals(JsonHelper.formatTimestamp(1386372357023L), "2013-12-06T23:25:57.023Z");
        assertEquals(JsonHelper.formatTimestamp(null), null);
    }

    @Test
    public void testParse() throws ParseException {
        assertEquals(JsonHelper.parseTimestamp("2013-12-06T23:25:57.023Z"), new Date(1386372357023L));
        assertEquals(JsonHelper.parseTimestamp(null), null);
    }
}
