package com.bazaarvoice.emodb.sor.delta.deser;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Writer;

/**
 * Helper class for serializing JSON within implementations of {@link com.bazaarvoice.emodb.sor.delta.Delta}.
 */
public abstract class DeltaJson {

    private static final MappingJsonFactory FACTORY;
    private static final ObjectMapper MAPPER;

    static {
        FACTORY = new MappingJsonFactory();
        FACTORY.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        try {
            FACTORY.configure(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM, false);
        } catch (NoSuchFieldError e) {
            // Ignore.  Old versions of Jackson (pre-1.7) are missing this field.  ETL Hadoop may use such old Jackson.
        }
        MAPPER = FACTORY.getCodec();
    }

    public static void write(Writer writer, Object value) throws IOException {
        MAPPER.writeValue(writer, value);
    }

    public static void write(Writer writer, String string) throws IOException {
        // For a simple string, bypass the ObjectMapper use the lower level JsonGenerator API (should be a bit faster)
        JsonGenerator gen = FACTORY.createGenerator(writer);
        gen.writeString(string);
        gen.close();
    }

    public static Appendable append(Appendable appendable, Object value) {
        try {
            write(asWriter(appendable), value);
        } catch (IOException e) {
            // Not expected writing to appendable
            throw new RuntimeException(e);
        }
        return appendable;
    }

    public static Appendable append(Appendable appendable, String string) {
        try {
            write(asWriter(appendable), string);
        } catch (IOException e) {
            // Not expected writing to appendable
            throw new RuntimeException(e);
        }
        return appendable;
    }

    private static Writer asWriter(final Appendable appendable) {
        return new Writer() {
            @Override
            public void write(char[] cbuf, int off, int len) throws IOException {
                for (int i = 0; i < len; i++) {
                    appendable.append(cbuf[off + i]);
                }
            }

            @Override
            public void write(String str) throws IOException {
                appendable.append(str);
            }

            @Override
            public void write(String str, int off, int len) throws IOException {
                appendable.append(str.substring(off, off + len));
            }

            @Override
            public Writer append(CharSequence chars) throws IOException {
                appendable.append(chars);
                return this;
            }

            @Override
            public Writer append(CharSequence chars, int start, int end) throws IOException {
                appendable.append(chars, start, end);
                return this;
            }

            @Override
            public Writer append(char c) throws IOException {
                return super.append(c);
            }

            @Override
            public void flush() throws IOException {
                // No-op
            }

            @Override
            public void close() throws IOException {
                // No-op
            }
        };
    }
}
