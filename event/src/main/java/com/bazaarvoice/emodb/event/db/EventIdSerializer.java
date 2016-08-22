package com.bazaarvoice.emodb.event.db;

public interface EventIdSerializer {
    /** Formats the specified event ID as a string. */
    String toString(EventId eventId);

    /** Parses the specified event ID string into an {@code EventId} object. */
    EventId fromString(String string, String channel);
}
