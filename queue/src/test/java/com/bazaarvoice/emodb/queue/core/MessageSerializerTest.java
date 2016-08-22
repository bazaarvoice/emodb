package com.bazaarvoice.emodb.queue.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.testng.Assert.assertEquals;

public class MessageSerializerTest {
    @Test
    public void testNull() {
        doRoundTrip(null);
    }

    @Test
    public void testBoolean() {
        doRoundTrip(false);
        doRoundTrip(true);
    }

    @Test
    public void testInteger() {
        doRoundTrip(0);
        doRoundTrip(1);
        doRoundTrip(Integer.MIN_VALUE);
        doRoundTrip(Integer.MAX_VALUE);
    }

    @Test
    public void testDecimal() {
        doRoundTrip(0.0);
        doRoundTrip(1.2);
        doRoundTrip(Double.MAX_VALUE);
        doRoundTrip(Double.MIN_VALUE);
    }

    @Test
    public void testString() {
        doRoundTrip("");
        doRoundTrip("hello world");
    }

    @Test
    public void testStringEscapes() {
        doRoundTrip("'\"\\[]{}, \t\n");
    }

    @Test
    public void testStringUnicode() {
        Random random = new Random();
        StringBuilder buf = new StringBuilder();
        while (buf.length() < 1000) {
            char ch = (char) random.nextInt(0xffff);
            if (Character.isDefined(ch)) {
                buf.append(ch);
            }
        }
        doRoundTrip(buf.toString());
    }

    @Test
    public void testArray() {
        doRoundTrip(ImmutableList.of(1, "two", ImmutableList.of(false, 4, 5)));
    }

    @Test
    public void testMap() {
        doRoundTrip(ImmutableMap.of("a", 1, "b", "c", "d", ImmutableMap.of("k", "v")));
    }

    private void doRoundTrip(Object expected) {
        ByteBuffer buf = MessageSerializer.toByteBuffer(expected);
        Object actual = MessageSerializer.fromByteBuffer(buf);
        assertEquals(actual, expected);
    }
}
