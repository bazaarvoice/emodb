package com.bazaarvoice.emodb.web.resources.blob;

import com.bazaarvoice.emodb.blob.api.RangeSpecification;
import com.bazaarvoice.emodb.blob.api.RangeSpecifications;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class RangeParamTest {

    @Test
    public void testSubset() {
        RangeSpecification range = new RangeParam("bytes=123-456").get();
        assertEquals(range, RangeSpecifications.slice(123, 334L));
    }

    @Test
    public void testPrefix() {
        RangeSpecification range = new RangeParam("bytes=123-").get();
        assertEquals(range, RangeSpecifications.slice(123));
    }

    @Test
    public void testSuffix() {
        RangeSpecification range = new RangeParam("bytes=-123").get();
        assertEquals(range, RangeSpecifications.suffix(123));
    }

    @Test
    public void testSingle() {
        RangeSpecification range = new RangeParam("bytes=2-2").get();
        assertEquals(range, RangeSpecifications.slice(2, 1L));
    }

    @Test
    public void testBackward() {
        assertNull(new RangeParam("bytes=2-1").get());
    }

    @Test
    public void testMissingStartEnd() {
        assertNull(new RangeParam("bytes=-").get());
    }

    @Test
    public void testOutOfRangeStart() {
        assertNull(new RangeParam("bytes=123456789012345678901234567890-").get());
    }

    @Test
    public void testOutOfRangeEnd() {
        assertNull(new RangeParam("bytes=-123456789012345678901234567890").get());
    }
}
