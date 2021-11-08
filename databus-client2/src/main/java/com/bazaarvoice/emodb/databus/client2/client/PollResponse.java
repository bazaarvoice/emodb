package com.bazaarvoice.emodb.databus.client2.client;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
import java.util.List;

/**
 * Response object for Databus poll requests.
 */
public class PollResponse {

    private final List<Event> _events;
    private final boolean _subscriptionEmpty;

    public PollResponse(List<Event> events, boolean subscriptionEmpty) {
        _events = events;
        _subscriptionEmpty = subscriptionEmpty;
    }

    public List<Event> getEvents() {
        return _events;
    }

    public boolean isSubscriptionEmpty() {
        return _subscriptionEmpty;
    }

    @JsonDeserialize(using = EventDeserializer.class)
    public static class Event {
        private final String _eventKey;
        private final String _content;


        public Event(String eventKey, String content) throws IOException {
            _eventKey = eventKey;
            _content = content;
        }
    }

    private static class EventDeserializer extends JsonDeserializer<Event> {
        @Override
        public Event deserialize(JsonParser parser, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            JsonToken token = parser.getCurrentToken();
            if (token != JsonToken.START_OBJECT) {
                throw ctxt.wrongTokenException(parser, JsonToken.START_OBJECT, "Map expected");
            }

            String eventKey = null;
            String content = null;

            while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                assert token == JsonToken.FIELD_NAME;

                String fieldName = parser.getText();
                parser.nextToken();

                if ("eventKey".equals(fieldName)) {
                    eventKey = parser.getValueAsString();
                } else if ("content".equals(fieldName)) {
                    content = parser.readValueAs(JsonNode.class).toString();
                } else {
                    parser.skipChildren();
                }
            }

            return new Event(eventKey, content);
        }
    }}
