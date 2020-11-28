package com.bazaarvoice.emodb.common.json;

import org.testng.annotations.Test;

import java.text.ParseException;
import java.util.Date;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

//Add tests
public class JsonHelperTest {
    @Test
    public void testFormat() {
        assertEquals(JsonHelper.formatTimestamp(new Date(1386372357023L)), "2013-12-06T23:25:57.023Z");
        assertEquals(JsonHelper.formatTimestamp(1386372357023L), "2013-12-06T23:25:57.023Z");
        assertNull(JsonHelper.formatTimestamp(null));
    }

    @Test
    public void testParse() throws ParseException {
        assertEquals(JsonHelper.parseTimestamp("2013-12-06T23:25:57.023Z"), new Date(1386372357023L));
        assertNull(JsonHelper.parseTimestamp(null));
    }

    @Test
    public void asJson() {
    }

    @Test
    public void asUtf8Bytes() {
    }

    @Test
    public void writeJson() {
    }

    @Test
    public void fromJson() {
    }

    @Test
    public void testFromJson() {
    }

    @Test
    public void fromUtf8Bytes() {
    }

    @Test
    public void readJson() {
    }

    @Test
    public void testReadJson() {
    }

    @Test
    public void convert() {
    }

    @Test
    public void testConvert() {
    }

    @Test
    public void parseTimestamp() {
    }

    @Test
    public void formatTimestamp() {
    }

    @Test
    public void testFormatTimestamp() {
    }

    @Test
    public void withView() {
    }
}
