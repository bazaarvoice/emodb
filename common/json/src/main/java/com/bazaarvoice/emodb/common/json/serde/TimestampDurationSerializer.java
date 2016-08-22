package com.bazaarvoice.emodb.common.json.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.joda.time.Duration;

import java.io.IOException;

/**
 * Overloads Duration default serializer from JodaModule to always serialize durations in milliseconds.
 */
public class TimestampDurationSerializer extends JsonSerializer<Duration> {

    @Override
    public void serialize(Duration duration, JsonGenerator generator, SerializerProvider serializerProvider)
            throws IOException {
        generator.writeNumber(duration.getMillis());
    }
}
