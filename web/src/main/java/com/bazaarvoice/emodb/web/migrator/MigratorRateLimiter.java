package com.bazaarvoice.emodb.web.migrator;

public interface MigratorRateLimiter {
    int getMaxWritesPerSecond(String migrationId);
}
