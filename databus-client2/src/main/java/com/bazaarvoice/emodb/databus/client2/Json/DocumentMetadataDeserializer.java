package com.bazaarvoice.emodb.databus.client2.Json;

import com.bazaarvoice.emodb.databus.client2.DocumentId;
import com.bazaarvoice.emodb.databus.client2.DocumentMetadata;
import com.bazaarvoice.emodb.databus.client2.DocumentVersion;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;

import java.io.IOException;
import java.text.ParsePosition;

/**
 * Efficient deserializer for {@link DocumentMetadata}.
 */
public class DocumentMetadataDeserializer extends JsonDeserializer<DocumentMetadata> {

    private final static ISO8601DateFormat _dateParser = new ISO8601DateFormat();

    @Override
    public DocumentMetadata deserialize(JsonParser jsonParser, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        JsonToken tok = jsonParser.getCurrentToken();
        if (tok != JsonToken.START_OBJECT) {
            throw ctxt.wrongTokenException(jsonParser, tok, "Json object required");
        }

        DocumentAttributes attrs = new DocumentAttributes();

        JsonToken token;
        while ((token = jsonParser.nextToken()) != JsonToken.END_OBJECT) {
            assert token == JsonToken.FIELD_NAME;

            String field = jsonParser.getText();
            token = jsonParser.nextToken();

            if (!attrs.allAttributesFound()) {
                if (attrs.table == null && "~table".equals(field) && token == JsonToken.VALUE_STRING) {
                    attrs.table = jsonParser.getValueAsString();
                } else if (attrs.key == null && "~id".equals(field) && token == JsonToken.VALUE_STRING) {
                    attrs.key = jsonParser.getValueAsString();
                } else if (attrs.version == null && "~version".equals(field) && token.isNumeric()) {
                    attrs.version = jsonParser.getValueAsLong();
                } else if (attrs.lastUpdateTs == null && "~lastUpdateAt".equals(field) && token == JsonToken.VALUE_STRING) {
                    attrs.lastUpdateTs = _dateParser.parse(jsonParser.getValueAsString(), new ParsePosition(0)).getTime();
                } else if (attrs.deleted == null && "~deleted".equals(field) && token.isBoolean()) {
                    attrs.deleted = jsonParser.getValueAsBoolean();
                } else {
                    skipFieldValue(token, jsonParser);
                }
            } else {
                skipFieldValue(token, jsonParser);
            }
        }

        if (!attrs.allAttributesFound()) {
            throw ctxt.mappingException("Docuemnt did not contain all expected intrinsics");
        }
        
        return new DocumentMetadata(
                new DocumentId(attrs.table, attrs.key), new DocumentVersion(attrs.version, attrs.lastUpdateTs), attrs.deleted);
    }

    private void skipFieldValue(JsonToken token, JsonParser jsonParser) throws IOException {
        if (token.isStructStart()) {
            jsonParser.skipChildren();
        }
    }

    private class DocumentAttributes {
        String table;
        String key;
        Long version;
        Long lastUpdateTs;
        Boolean deleted;

        boolean allAttributesFound() {
            return table != null && key != null && version != null && lastUpdateTs != null && deleted != null;
        }
    }
}
