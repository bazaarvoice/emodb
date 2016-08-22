package com.bazaarvoice.emodb.event.db.astyanax;

import com.bazaarvoice.emodb.event.db.EventId;
import com.bazaarvoice.emodb.event.db.EventIdSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;

import java.nio.ByteBuffer;

public class AstyanaxEventIdSerializer implements EventIdSerializer {
    @Override
    public String toString(EventId eventId) {
        BytesArraySerializer serializer = BytesArraySerializer.get();
        return serializer.getString(ByteBuffer.wrap(eventId.array()));
    }

    @Override
    public EventId fromString(String string, String channel) {
        BytesArraySerializer serializer = BytesArraySerializer.get();
        return AstyanaxEventId.parse(serializer.fromByteBuffer(serializer.fromString(string)), channel);
    }
}
