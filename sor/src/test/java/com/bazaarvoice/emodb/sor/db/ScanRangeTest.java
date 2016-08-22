package com.bazaarvoice.emodb.sor.db;

import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static com.bazaarvoice.emodb.sor.db.ScanRange.create;
import static org.testng.Assert.assertEquals;

public class ScanRangeTest {

    @Test
    public void testIntersection() {
        ByteBuffer a = ByteBuffer.wrap(BaseEncoding.base16().decode("010000000000000064666F6F"));
        ByteBuffer b = ByteBuffer.wrap(BaseEncoding.base16().decode("030000000000000064666F6F"));
        ByteBuffer c = ByteBuffer.wrap(BaseEncoding.base16().decode("050000000000000064666F6F"));
        ByteBuffer d = ByteBuffer.wrap(BaseEncoding.base16().decode("070000000000000064666F6F"));
        ByteBuffer e = ByteBuffer.wrap(BaseEncoding.base16().decode("090000000000000064666F6F"));

        List<ScanRange> empty = ImmutableList.of();

        // Non-wrapping tests

        // Equality
        assertEquals(create(a, b).intersection(create(a, b)), ImmutableList.of(create(a, b)));
        // Shared start
        assertEquals(create(a, c).intersection(create(a, b)), ImmutableList.of(create(a, b)));
        // Shared end
        assertEquals(create(a, c).intersection(create(b, c)), ImmutableList.of(create(b, c)));
        // Partial overlap
        assertEquals(create(a, c).intersection(create(b, d)), ImmutableList.of(create(b, c)));
        // No overlap
        assertEquals(create(a, b).intersection(create(c, d)), empty);
        // Shared endpoint but no overlap
        assertEquals(create(a, b).intersection(create(b, c)), empty);

        // Wrapping tests

        // Complete range, same endpoint
        assertEquals(create(a, a).intersection(create(a, a)), ImmutableList.of(ScanRange.all()));
        // Complete range, different endpoint
        assertEquals(create(a, a).intersection(create(b, b)), ImmutableList.of(ScanRange.all()));
        // Partial overlap with low-end
        assertEquals(create(a, d).intersection(create(e, c)), ImmutableList.of(create(a, c)));
        // Complete overlap with low-end
        assertEquals(create(a, b).intersection(create(d, c)), ImmutableList.of(create(a, b)));
        // Partial overlap with high-end
        assertEquals(create(b, e).intersection(create(c, a)), ImmutableList.of(create(c, e)));
        // Complete overlap with high-end
        assertEquals(create(d, e).intersection(create(c, b)), ImmutableList.of(create(d, e)));
        // Double overlapping with partial overlap
        assertEquals(create(d, a).intersection(create(e, b)), ImmutableList.of(create(e, a)));
        // Double overlapping with complete overlap
        assertEquals(create(d, b).intersection(create(e, a)), ImmutableList.of(create(e, a)));
        // Partial overlap on both ends
        assertEquals(create(a, e).intersection(create(d, b)), ImmutableList.of(create(a, b), create(d, e)));
        // No overlap
        assertEquals(create(b, c).intersection(create(e, a)), empty);
        // Shared endpoints but no overlap
        assertEquals(create(b, c).intersection(create(c, b)), empty);
    }
}
