package com.bazaarvoice.emodb.databus.core;

import java.time.Instant;

/**
 * Event thrown when a databus event exists for an unknown table.  Typically this happens when the table has been dropped
 * after a document in the table was updated but before the event associated with that update is fanned out.  There
 * also exists a race condition where document updates written immediately after a table is created may reach fanout
 * before the table cache local to fanout has been properly updated.  To assist with this case the event's timestamp
 * is included in the event so fanout can delay deleting the event until enough time has passed to be confident the
 * event is orphaned because of the expected case where the table has been dropped.
 */
public class OrphanedEventException extends Exception {

    private String _table;
    private Instant _eventTime;

    public OrphanedEventException(String table, Instant eventTime) {
        super("Event for unknown table: " + table);
        _table = table;
        _eventTime = eventTime;
    }

    public String getTable() {
        return _table;
    }

    public Instant getEventTime() {
        return _eventTime;
    }
}
