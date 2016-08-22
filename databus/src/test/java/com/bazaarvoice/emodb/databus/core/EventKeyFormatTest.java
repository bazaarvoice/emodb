package com.bazaarvoice.emodb.databus.core;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class EventKeyFormatTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEncodeNone() {
        EventKeyFormat.encode(Collections.<String>emptyList());
    }

    @Test
    public void testDecodeNone() {
        assertEquals(Collections.<String>emptyList(), EventKeyFormat.decodeAll(Collections.<String>emptyList()));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEncodeEmpty() {
        EventKeyFormat.encode(ImmutableList.of(""));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDecodeEmpty() {
        EventKeyFormat.decodeAll(ImmutableList.of(""));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDecodeOneNonHex() {
        EventKeyFormat.decodeAll(ImmutableList.of("g"));
    }

    @Test
    public void testOne() {
        doRoundTrip("0a", "0a");
    }

    @Test
    public void testTwoRegular() {
        doRoundTrip("12" + "If2",
                "12",
                "f2");
    }

    @Test
    public void testTwoSharedPrefix() {
        doRoundTrip("012" + "Xf",
                "012",
                "01f");
    }

    @Test
    public void testMixedSharedAndRegular() {
        doRoundTrip("0123456789abcdef" + "X0" + "I543",
                "0123456789abcdef",
                "0123456789abcde0",
                "543");
    }

    @Test
    public void testMixedRegularAndShared() {
        doRoundTrip("543" + "I0123456789abcdef" + "X0",
                "543",
                "0123456789abcdef",
                "0123456789abcde0");
    }

    @Test
    public void testLongPrefix() {
        doRoundTrip("0123456789abcdef0123456789abcdef0123456789ab0000" + "X1" + "X13" + "Xf" + "X445",
                "0123456789abcdef0123456789abcdef0123456789ab0000",
                "0123456789abcdef0123456789abcdef0123456789ab0001",
                "0123456789abcdef0123456789abcdef0123456789ab0013",
                "0123456789abcdef0123456789abcdef0123456789ab001f",
                "0123456789abcdef0123456789abcdef0123456789ab0445");
    }

    @Test
    public void testLongSuffix() {
        doRoundTrip("0123456789abcdef0123456789abcdef0123456789abcdef" + "Xf23456789abcdef0123456789abcdef",
                "0123456789abcdef0" + "123456789abcdef0123456789abcdef",
                "0123456789abcdef0" + "f23456789abcdef0123456789abcdef");
        // different character --------^
    }

    @Test
    public void testLonger() {
        // After a length change emit the entire string
        doRoundTrip("0123456789abcdef0123456789abcdef0123456789ab0000" + "I0123456789abcdef0123456789abcdef0123456789ab00004" + "X5",
                "0123456789abcdef0123456789abcdef0123456789ab0000",
                "0123456789abcdef0123456789abcdef0123456789ab00004",
                "0123456789abcdef0123456789abcdef0123456789ab00005");
    }

    @Test
    public void testShorter() {
        // After a length change emit the entire string
        doRoundTrip("0123456789abcdef0123456789abcdef0123456789ab00005" + "I0123456789abcdef0123456789abcdef0123456789ab0004" + "X0",
                "0123456789abcdef0123456789abcdef0123456789ab00005",
                "0123456789abcdef0123456789abcdef0123456789ab0004",
                "0123456789abcdef0123456789abcdef0123456789ab0000");
    }

    @Test
    public void testDuplicateStrings() {
        // Not supposed to happen, but might as well test that it works...
        doRoundTrip("0123456789XXXXXX",
                "0123456789", "0123456789", "0123456789", "0123456789", "0123456789", "0123456789", "0123456789");
    }

    @Test
    public void testRealIds() {
        // This tests a sequence of real event IDs.
        doRoundTrip(
                "f6377a9029fa11e285b7c82a1442d50e002aed1fX5c8d54X8ef30dXc084c3Xf2650dX124bdebX56e4a1X88260eXbad614Xecac52",
                "f6377a9029fa11e285b7c82a1442d50e002aed1f",
                "f6377a9029fa11e285b7c82a1442d50e005c8d54",
                "f6377a9029fa11e285b7c82a1442d50e008ef30d",
                "f6377a9029fa11e285b7c82a1442d50e00c084c3",
                "f6377a9029fa11e285b7c82a1442d50e00f2650d",
                "f6377a9029fa11e285b7c82a1442d50e0124bdeb",
                "f6377a9029fa11e285b7c82a1442d50e0156e4a1",
                "f6377a9029fa11e285b7c82a1442d50e0188260e",
                "f6377a9029fa11e285b7c82a1442d50e01bad614",
                "f6377a9029fa11e285b7c82a1442d50e01ecac52");
    }

    private void doRoundTrip(String expectedEventKey, String... eventIds) {
        String actualEventKey = EventKeyFormat.encode(Arrays.asList(eventIds));
        assertEquals(actualEventKey, expectedEventKey);

        List<String> actualEventIds = EventKeyFormat.decodeAll(ImmutableList.of(actualEventKey));
        assertEquals(actualEventIds, Arrays.asList(eventIds));
    }
}
