package com.bazaarvoice.emodb.queue.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public final class Message {
    private final String _id;
    private final Object _payload;

    public Message(@JsonProperty("id") String id, @JsonProperty("payload") @Nullable Object payload) {
        _id = requireNonNull(id, "id");
        _payload = payload;
    }

    public String getId() {
        return _id;
    }

    public Object getPayload() {
        return _payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Message)) {
            return false;
        }
        Message message = (Message) o;
        return _id.equals(message.getId()) &&
                Objects.equal(_payload, message.getPayload());
    }

    @Override
    public int hashCode() {
        return _id.hashCode();
    }

    @Override
    public String toString() {
        return "Message[" + _id + ", " + _payload + "]";
    }
}
