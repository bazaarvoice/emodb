package com.bazaarvoice.emodb.blob.api;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.time.ZoneOffset;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;

public class BlobMetadataJsonTest {

    @Test
    public void testRoundTrip() {
        Date timestamp = new Date();
        String timestampString = StdDateFormat.getISO8601Format(TimeZone.getTimeZone(ZoneOffset.UTC), Locale.ENGLISH).format(timestamp).replaceFirst("Z$", "+0000");

        BlobMetadata expected = new DefaultBlobMetadata("id", timestamp,
                1234, "1e00d0c82221522ce2cf80365dc1fbfc",
                "6c4ebe94eb98c9c790ed89e48e581428d1b65b0f", ImmutableMap.of("contentType", "image/jpeg"));
        String string = JsonHelper.asJson(expected);

        assertEquals(string, "{\"id\":\"id\",\"timestamp\":\"" + timestampString + "\",\"length\":1234," +
                "\"md5\":\"1e00d0c82221522ce2cf80365dc1fbfc\",\"sha1\":\"6c4ebe94eb98c9c790ed89e48e581428d1b65b0f\"," +
                "\"attributes\":{\"contentType\":\"image/jpeg\"}}");

        BlobMetadata actual = JsonHelper.fromJson(string, DefaultBlobMetadata.class);

        assertEquals(actual.getId(), expected.getId());
        assertEquals(actual.getLength(), expected.getLength());
        assertEquals(actual.getMD5(), expected.getMD5());
        assertEquals(actual.getSHA1(), expected.getSHA1());
        assertEquals(actual.getAttributes(), expected.getAttributes());
    }
}
