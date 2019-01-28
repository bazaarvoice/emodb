package com.bazaarvoice.emodb.common.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import org.apache.http.MalformedChunkCodingException;
import org.apache.http.TruncatedChunkException;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class JsonStreamingArrayParserTest {

    @Test
    public void testParsing() {
        Iterator<Integer> iter = newParser(stream("[1,4,2,3]"), Integer.class);
        assertEquals((int) iter.next(), 1);
        assertEquals((int) iter.next(), 4);
        assertEquals((int) iter.next(), 2);
        assertEquals((int) iter.next(), 3);
        assertFalse(iter.hasNext());
    }

    @Test
    public void testMalformedJson() {
        // If the first character in the stream isn't valid (ie. '[') then assume we have a malformed response,
        // not that we encountered an early EOF.  It's an ambiguous situation, but the former is more likely.
        // Once we get past the first character it's likely that the rest is valid json (our server doesn't emit
        // invalid json!) so presume that parse exceptions are due to early EOF.
        try {
            newParser(stream(""), String.class);
            fail();
        } catch (RuntimeException e) {
            assertEquals(e.getClass(), RuntimeException.class);  // Should not be JsonStreamingEOFException
            assertTrue(e.getCause() instanceof JsonParseException);
        }

        try {
            newParser(stream("{\"key\":\"value\"}"), String.class);
            fail();
        } catch (RuntimeException e) {
            assertEquals(e.getClass(), RuntimeException.class);  // Should not be JsonStreamingEOFException
            assertTrue(e.getCause() instanceof JsonParseException);
        }
    }

    @Test
    public void testEOFException() {
        assertThrowsEOFException(stream("["), Integer.class);
        assertThrowsEOFException(stream("[5"), Integer.class);
        assertThrowsEOFException(stream("[5,"), Integer.class);
        assertThrowsEOFException(stream("[5,6"), Integer.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMalformedChunkException() throws Exception {
        ByteSource input = ByteSource.concat(
                ByteSource.wrap("[5,6".getBytes(Charsets.UTF_8)),
                exceptionStreamSupplier(new MalformedChunkCodingException("Bad chunk header")));
        assertThrowsEOFException(input.openStream(), Integer.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTruncatedChunkException() throws Exception {
        ByteSource input = ByteSource.concat(
                ByteSource.wrap("[5,6".getBytes(Charsets.UTF_8)),
                exceptionStreamSupplier(new TruncatedChunkException("Truncated chunk ( expected size: 3996; actual size: 1760)")));
        assertThrowsEOFException(input.openStream(), Integer.class);
    }

    @Test
    public void testJsonMapperException() throws Exception {
        assertThrowsProcessingException(stream("[\"seven\"]"), Integer.class);
    }

    @Test
    public void testJsonCreationException() throws Exception {
        assertThrowsProcessingException(stream("[{\"foo\":\"bar\"}]"), UncreatableObject.class);
    }

    public static class UncreatableObject {
        @JsonCreator
        public UncreatableObject(@JsonProperty ("foo") String foo) {
            throw new IllegalArgumentException("always fails to create");
        }
    }

    private <T> void assertThrowsEOFException(InputStream in, Class<T> type) {
        assertThrowsException(in, type, JsonStreamingEOFException.class);
    }

    private <T> void assertThrowsProcessingException(InputStream in, Class<T> type) {
        assertThrowsException(in, type, JsonStreamProcessingException.class);
    }

    private <T, E extends Exception> void assertThrowsException(InputStream in, Class<T> type, Class<E> exceptionClass) {
        Iterator<T> iter = newParser(in, type);
        try {
            while (iter.hasNext()) {
                iter.next();
            }
            fail();
        } catch (Exception e) {
            if (!exceptionClass.isInstance(e)) {
                // Unexpected exception
                throw Throwables.propagate(e);
            }
        }
    }

    private <T> Iterator<T> newParser(InputStream in, Class<T> type) {
        return new JsonStreamingArrayParser<>(in, type);
    }

    private InputStream stream(String json) {
        return new ByteArrayInputStream(json.getBytes(Charsets.UTF_8));
    }

    private ByteSource exceptionStreamSupplier(final Throwable t) {
        return new ByteSource() {
            @Override
            public InputStream openStream() throws IOException {
                return exceptionStream(t);
            }
        };
    }

    private InputStream exceptionStream(final Throwable t) {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                Throwables.propagateIfInstanceOf(t, IOException.class);
                throw Throwables.propagate(t);
            }
        };
    }
}