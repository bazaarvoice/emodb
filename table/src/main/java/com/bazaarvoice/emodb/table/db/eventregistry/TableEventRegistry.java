package com.bazaarvoice.emodb.table.db.eventregistry;

import java.time.Instant;
import java.util.Map;

public interface TableEventRegistry {
    void registerTableListener(String registrationId, Instant newExpirationTime);

    void markTableEventAsComplete(String registrationId, String table, String uuid);

    Map.Entry<String, TableEvent> getNextReadyTableEvent(String registrationId);
}
