package com.bazaarvoice.emodb.sor.condition.deser;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaParser;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

/**
 * Jackson adapter for deserializing stringified {@link com.bazaarvoice.emodb.sor.delta.Delta} objects.
 */
public class ConditionDeserializer extends JsonDeserializer<Condition> {

    @Override
    public Condition deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        JsonToken tok = jp.getCurrentToken();
        if (tok != JsonToken.VALUE_STRING) {
            throw ctxt.wrongTokenException(jp, tok, "Could not map to a Condition string.");
        }

        String string = jp.getText();
        try {
            return DeltaParser.parseCondition(string);
        } catch (Exception e) {
            throw ctxt.weirdStringException(string, Condition.class, "Not a valid Condition string representation.");
        }
    }
}
