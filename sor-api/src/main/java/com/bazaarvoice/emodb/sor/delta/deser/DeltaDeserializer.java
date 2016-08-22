package com.bazaarvoice.emodb.sor.delta.deser;

import com.bazaarvoice.emodb.sor.delta.Delta;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

/**
 * Jackson adapter for deserializing stringified {@link Delta} objects.
 */
public class DeltaDeserializer extends JsonDeserializer<Delta> {

    @Override
    public Delta deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        JsonToken tok = jp.getCurrentToken();
        if (tok != JsonToken.VALUE_STRING) {
            throw ctxt.wrongTokenException(jp, tok, "Could not map to a Delta string.");
        }

        String string = jp.getText();
        try {
            return DeltaParser.parse(string);
        } catch (Exception e) {
            throw ctxt.weirdStringException(string, Delta.class, "Not a valid Delta string representation.");
        }
    }
}
