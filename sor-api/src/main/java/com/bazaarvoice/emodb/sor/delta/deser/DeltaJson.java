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
        try (JsonGenerator gen = FACTORY.createJsonGenerator(writer)) {
            gen.writeString(string);
        }
    }
}
