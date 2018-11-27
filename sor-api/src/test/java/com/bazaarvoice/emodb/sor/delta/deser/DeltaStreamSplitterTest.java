package com.bazaarvoice.emodb.sor.delta.deser;

import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.StringReader;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class DeltaStreamSplitterTest {
    @Test
    public void testEmpty() {
        assertFalse(splitter("").hasNext());
    }

    @Test
    public void testWhitespace() {
        assertFalse(splitter(" \t \n ").hasNext());
    }

    @Test
    public void testEmptyArray() {
        assertFalse(splitter("[]").hasNext());
    }

    @Test
    public void testWhitespaceArray() {
        assertFalse(splitter(" \t [ \n ] ").hasNext());
    }

    @Test(expectedExceptions = ParseException.class)
    public void testEmptyArrayUnclosed() {
        splitter("[").next();
    }

    @Test(expectedExceptions = ParseException.class)
    public void testSingletonArrayUnclosed() {
        splitter("[1").next();
    }

    @Test(expectedExceptions = ParseException.class)
    public void testSingletonCommaArrayUnclosed() {
        splitter("[1,").next();
    }

    @Test(expectedExceptions = ParseException.class)
    public void testUnterminatedString1() {
        splitter("\"").next();
    }

    @Test(expectedExceptions = ParseException.class)
    public void testUnterminatedString2() {
        splitter("\"\\").next();
    }

    @Test
    public void testSingletonNumber() {
        assertEquals(parse("1"), ImmutableList.of(Deltas.literal(1)));
    }

    @Test
    public void testSingletonNumberArray() {
        assertEquals(parse("[1]"), ImmutableList.of(Deltas.literal(1)));
    }

    @Test
    public void testMultiple() {
        assertEquals(parse("1 ~ .. true {..,\"x \\\" y\":[false,1.0]}\nnull\n{} [1,2,3,[[]]]"),
                ImmutableList.of(
                        Deltas.literal(1),
                        Deltas.delete(),
                        Deltas.noop(),
                        Deltas.literal(true),
                        Deltas.mapBuilder().put("x \" y", ImmutableList.of(false, 1.0)).build(),
                        Deltas.literal(null),
                        Deltas.literal(Collections.emptyMap()),
                        Deltas.literal(ImmutableList.of(1, 2, 3, ImmutableList.of(ImmutableList.of())))
                ));
    }

    @Test
    public void testMultipleArray() {
        assertEquals(parse("[1,~,..,true,{..,\"x \\\" y\":[false,1.0]},null,{},[1,2,3,[[]]]]"),
                ImmutableList.of(
                        Deltas.literal(1),
                        Deltas.delete(),
                        Deltas.noop(),
                        Deltas.literal(true),
                        Deltas.mapBuilder().put("x \" y", ImmutableList.of(false, 1.0)).build(),
                        Deltas.literal(null),
                        Deltas.literal(Collections.emptyMap()),
                        Deltas.literal(ImmutableList.of(1, 2, 3, ImmutableList.of(ImmutableList.of())))
                ));
    }

    private List<Delta> parse(String string) {
        ImmutableList.Builder<Delta> list = ImmutableList.builder();
        for (Iterator<String> it = splitter(string); it.hasNext(); ) {
            list.add(Deltas.fromString(it.next()));
        }
        return list.build();
    }

    private Iterator<String> splitter(String string) {
        return StreamSupport.stream(new DeltaStreamSplitter(new StringReader(string)), false).iterator();
    }
}
