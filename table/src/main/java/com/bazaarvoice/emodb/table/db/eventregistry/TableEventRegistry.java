package com.bazaarvoice.emodb.table.db.eventregistry;

import java.time.Instant;

public interface TableEventRegistry {
    void registerTableListener(String registrationId, Instant newExpirationTime);
}
