package com.bazaarvoice.emodb.table.db;

import java.util.Set;
import java.util.UUID;

/**
 * Internal interface that may be used for the specified delete operations usually on all the keys of a table.
 */
public interface TableDeleteOperations {
    /* to send databus events for all the keys of the table with the given changeId and tags. */
    void sendDeleteEventsForTable(String table, UUID changeId, Set<String> tags);
}