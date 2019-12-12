package com.bazaarvoice.emodb.table.db.eventregistry;

import java.time.Instant;
import java.util.Map;

public interface TableEventRegistry {
    void registerTableListener(String registrationId, Instant newExpirationTime);

    Map.Entry<String, TableEvent> getNextTableEvent(String registrationId);
}
