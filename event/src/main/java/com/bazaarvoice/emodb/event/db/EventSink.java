package com.bazaarvoice.emodb.event.db;

import java.nio.ByteBuffer;

public interface EventSink {

    /** Returns true to keep searching for more events, false to stop searching for events. */
    boolean accept(EventId eventId, ByteBuffer eventData);
}
