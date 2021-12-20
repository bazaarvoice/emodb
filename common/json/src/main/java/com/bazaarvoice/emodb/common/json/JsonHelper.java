package com.bazaarvoice.emodb.common.json;

import com.bazaarvoice.emodb.common.json.deferred.LazyJsonModule;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ISO8601Utils;
import com.google.common.base.Throwables;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.ParsePosition;
import java.util.Date;

public abstract class JsonHelper {

    private static final ObjectMapper JSON = CustomJsonObjectMapperFactory.build()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .registerModule(new LazyJsonModule());

    private static final ObjectWriter DEFAULT_WRITER = JSON.writer();

    public static String asJson(Object value) {
        return asJson(value, DEFAULT_WRITER);
    }

    private static String asJson(Object value, ObjectWriter writer) {
        try {
            return writer.writeValueAsString(value);
        } catch (IOException e) {
            // Shouldn't get I/O errors writing to a string.
            Throwables.propagateIfPossible(e);
            throw new RuntimeException(e);
        }
    }

    public static byte[] asUtf8Bytes(Object value) {
        return asUtf8Bytes(value, DEFAULT_WRITER);
    }

    private static byte[] asUtf8Bytes(Object value, ObjectWriter writer) {

        try {
            return writer.writeValueAsBytes(value);
        } catch (IOException e) {
            // Shouldn't get I/O errors writing to a string.
            Throwables.propagateIfPossible(e);
            throw new RuntimeException(e);
        }
    }

    public static void writeJson(OutputStream out, Object value)
            throws IOException {
        writeJson(out, value, DEFAULT_WRITER);
    }

    private static void writeJson(OutputStream out, Object value, ObjectWriter writer)
            throws IOException {
        writer.writeValue(out, value);
    }

    public static <T> T fromJson(String string, Class<T> valueType) {
        try {
            return JSON.readValue(string, valueType);
        } catch (IOException e) {
            // Must be malformed JSON.  Other kinds of I/O errors don't get thrown when reading from a string.
            throw new IllegalArgumentException(e.toString());
        }
    }

    public static <T> T fromJson(String string, TypeReference<T> reference) {
        try {
            return JSON.readValue(string, reference);
        } catch (IOException e) {
            // Must be malformed JSON.  Other kinds of I/O errors don't get thrown when reading from a string.
            throw new IllegalArgumentException(e.toString());
        }
    }

    public static <T> T fromUtf8Bytes(byte[] bytes, int offset, int length, Class<T> valueType) {
        try {
            return JSON.readValue(bytes, offset, length, valueType);
        } catch (IOException e) {
            // Must be malformed JSON.  Other kinds of I/O errors don't get thrown when reading from bytes.
            throw new IllegalArgumentException(e.toString());
        }
    }

    public static <T> T readJson(InputStream in, Class<T> valueType)
            throws IOException {
        return JSON.readValue(in, valueType);
    }

    public static <T> T readJson(InputStream in, TypeReference<T> reference)
            throws IOException {
        return JSON.readValue(in, reference);
    }

    /** Convert from one pojo format to another pojo format. */
    public static <T> T convert(Object source, Class<T> destType) {
        return JSON.convertValue(source, destType);
    }

    /** Convert from one pojo format to another pojo format. */
    public static <T> T convert(Object source, TypeReference<T> destType) {
        return JSON.convertValue(source, destType);
    }

    /** Parses an ISO 8601 date+time string into a Java Date object. */
    public static Date parseTimestamp(@Nullable String string) {
        Date date = null;
        try {
            if (string != null) {
                date = ISO8601Utils.parse(string, new ParsePosition(0));
            }
        } catch (ParseException e) {
            Throwables.propagateIfPossible(e);
            throw new RuntimeException(e);
        }
        return date;
    }

    /** Formats the specified timestamp as an ISO 8601 string with milliseconds and UTC timezone. */
    public static String formatTimestamp(@Nullable Date date) {
        return (date != null) ? date.toInstant().toString() : null;
    }

    /** Formats the specified timestamp as an ISO 8601 string with milliseconds and UTC timezone. */
    public static String formatTimestamp(long millis) {
        return ISO8601Utils.format(new Date(millis), true);
    }

    /**
     * Returns a helper class that can be used to write JSON using a specific JSON view.  For example:
     *
     * <code>JsonHelper.withView(PublicView.class).asJson(instance)</code>
     */
    public static JsonWriterWithViewHelper withView(Class view) {
        return new JsonWriterWithViewHelper(JSON.writerWithView(view));
    }

    public static class JsonWriterWithViewHelper {
        private final ObjectWriter _writer;

        private JsonWriterWithViewHelper(ObjectWriter writer) {
            _writer = writer;
        }

        public String asJson(Object value) {
            return JsonHelper.asJson(value, _writer);
        }

        public byte[] asUtf8Bytes(Object value) {
            return JsonHelper.asUtf8Bytes(value, _writer);
        }

        public void writeJson(OutputStream out, Object value)
                throws IOException {
            JsonHelper.writeJson(out, value, _writer);
        }
    }
}
