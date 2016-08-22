package com.bazaarvoice.emodb.event.db;

public interface EventId {
    /** Returns a shared reference to the event ID formatted as a byte array. */
    byte[] array();
}
