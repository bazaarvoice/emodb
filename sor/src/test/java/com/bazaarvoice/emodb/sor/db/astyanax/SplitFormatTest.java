package com.bazaarvoice.emodb.sor.db.astyanax;

import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.util.ByteBufferRangeImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.testng.Assert.assertEquals;

public class SplitFormatTest {

    private static final Random _random = new Random();

    @DataProvider
    public ByteBufferRange[][] randomRanges() {
        return new ByteBufferRange[][] {
                {toRange(randomBytes(0), randomBytes(0))},
                {toRange(randomBytes(0), randomBytes(1))},
                {toRange(randomBytes(1), randomBytes(0))},
                {toRange(randomBytes(1), randomBytes(1))},
                {toRange(randomBytes(_random.nextInt(32)), randomBytes(_random.nextInt(32)))},
                {randomRange(20, 0, 0)},
                {randomRange(20, 0, 1)},
                {randomRange(20, 1, 0)},
                {randomRange(20, 1, 1)},
                {randomRange(_random.nextInt(32), _random.nextInt(32), _random.nextInt(32))},
        };
    }

    @Test(dataProvider= "randomRanges")
    public void testEncodeDecodeRoundTrip(ByteBufferRange range) {
        assertRangeEquals(SplitFormat.decode(SplitFormat.encode(range)), range);
    }

    @Test
    public void testEncodeEmpty() {
        assertEquals(SplitFormat.encode(toRange(b(), b())), ":-");
    }

    @Test
    public void testDecodeEmpty() {
        assertRangeEquals(SplitFormat.decode(":-"), toRange(b(), b()));
    }

    @Test
    public void testEncodeSimple() {
        assertEquals(SplitFormat.encode(toRange(b(0x1, 0x2f), b(0x45, 0x67, 0xe9))), ":012f-4567e9");
    }

    @Test
    public void testDecodeSimple() {
        assertRangeEquals(SplitFormat.decode(":012f-4567e9"), toRange(b(0x1, 0x2f), b(0x45, 0x67, 0xe9)));
    }

    @Test
    public void testEncodePrefixOnly() {
        assertEquals(SplitFormat.encode(toRange(b(0x45, 0x67, 0x89, 0xab), b(0x45, 0x67, 0x89, 0xab))), "456789ab:-");
    }

    @Test
    public void testDecodePrefixOnly() {
        assertRangeEquals(SplitFormat.decode("456789ab:-"), toRange(b(0x45, 0x67, 0x89, 0xab), b(0x45, 0x67, 0x89, 0xab)));
    }

    @Test
    public void testEncodePrefixGranularity() {
        // Note: encode/decode common prefixes are calculated at the character granularity, not byte granularity.
        assertEquals(SplitFormat.encode(toRange(b(0x1, 0x23), b(0x4, 0x56))), "0:123-456");
    }

    @Test
    public void testDecodePrefixGranularity() {
        assertRangeEquals(SplitFormat.decode("01:23-45"), toRange(b(0x1, 0x23), b(0x1, 0x45)));
    }

    @Test
    public void testEncodeWithPrefix() {
        assertEquals(SplitFormat.encode(toRange(b(0x1, 0x23), b(0x1, 0x45))), "01:23-45");
    }

    @Test
    public void testDecodeWithPrefix() {
        assertRangeEquals(SplitFormat.decode("01:23-45"), toRange(b(0x1, 0x23), b(0x1, 0x45)));
    }

    @Test
    public void testDecodeShort() {
        assertRangeEquals(SplitFormat.decode("0:1-2"), toRange(b(0x1), b(0x2)));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDecodeInvalidEmpty() {
        SplitFormat.decode("");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDecodeMissingPrefix() {
        SplitFormat.decode("-");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDecodeMissingSep() {
        SplitFormat.decode(":");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDecodeShortPrefix() {
        SplitFormat.decode("0:-");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDecodeShortStart() {
        SplitFormat.decode("01:2-34");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDecodeShortEnd() {
        SplitFormat.decode("01:23-4");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDecodeBadHex1() {
        SplitFormat.decode("01:23-4z");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDecodeBadHex2() {
        SplitFormat.decode("g1:23-45");
    }

    private void assertRangeEquals(ByteBufferRange actual, ByteBufferRange expected) {
        assertEquals(actual.getStart(), expected.getStart());
        assertEquals(actual.getEnd(), expected.getEnd());
        assertEquals(actual.getLimit(), expected.getLimit());
        assertEquals(actual.isReversed(), expected.isReversed());
    }

    private ByteBufferRange toRange(ByteBuffer start, ByteBuffer end) {
        return new ByteBufferRangeImpl(start, end, -1, false);
    }

    /**
     * Creates a byte buffer literal.
     */
    private ByteBuffer b(int... bytes) {
        // add random prefix and suffix to help test that ByteBuffer position, limit, arrayOffset etc. are used correctly
        int prefix = _random.nextInt(5);
        int suffix = _random.nextInt(5);
        byte[] result = new byte[prefix + bytes.length + suffix];
        _random.nextBytes(result);  // fill prefix and suffix with random data
        for (int i = 0; i < bytes.length; i++) {
            result[prefix + i] = (byte) bytes[i];
        }
        return ByteBuffer.wrap(result, prefix, bytes.length);
    }

    private ByteBufferRange randomRange(int commonLen, int remainingFirstLen, int remainingSecondLen) {
        ByteBuffer first = randomBytes(commonLen + remainingFirstLen);
        ByteBuffer second = randomBytes(commonLen + remainingSecondLen);
        for (int i = 0; i < commonLen; i++) {
            second.put(second.position() + i, first.get(first.position() + i));
        }
        return toRange(first, second);
    }

    private ByteBuffer randomBytes(int len) {
        byte[] bytes = new byte[len];
        _random.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }
}
