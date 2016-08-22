package com.bazaarvoice.emodb.blob.api;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class RangeSpecificationTest {

    @Test
    public void testAll() {
        RangeSpecification spec = RangeSpecifications.slice(0);

        assertEquals(spec.toString(), "bytes=0-");

        assertUnsatisfiable(spec, 0);
        assertEquals(spec.getRange(1), new Range(0, 1));
        assertEquals(spec.getRange(20), new Range(0, 20));
    }

    @Test
    public void testOffset() {
        RangeSpecification spec = RangeSpecifications.slice(1);

        assertEquals(spec.toString(), "bytes=1-");

        assertUnsatisfiable(spec, 0);
        assertUnsatisfiable(spec, 1);
        assertEquals(spec.getRange(2), new Range(1, 1));
        assertEquals(spec.getRange(20), new Range(1, 19));
    }

    @Test
    public void testSubset() {
        RangeSpecification spec = RangeSpecifications.slice(2, 5);

        assertEquals(spec.toString(), "bytes=2-6");

        assertUnsatisfiable(spec, 0);
        assertUnsatisfiable(spec, 1);
        assertUnsatisfiable(spec, 2);
        assertEquals(spec.getRange(3), new Range(2, 1));
        assertEquals(spec.getRange(6), new Range(2, 4));
        assertEquals(spec.getRange(7), new Range(2, 5));
        assertEquals(spec.getRange(20), new Range(2, 5));
    }

    @Test
    public void testSuffix() {
        RangeSpecification spec = RangeSpecifications.suffix(5);

        assertEquals(spec.toString(), "bytes=-5");

        assertUnsatisfiable(spec, 0);

        assertEquals(spec.getRange(1), new Range(0, 1));
        assertEquals(spec.getRange(4), new Range(0, 4));
        assertEquals(spec.getRange(5), new Range(0, 5));
        assertEquals(spec.getRange(20), new Range(15, 5));
    }

    private void assertUnsatisfiable(RangeSpecification spec, long blobLength) {
        try {
            spec.getRange(blobLength);
            fail();
        } catch (RangeNotSatisfiableException e) {
            // expected
        }
    }
}
