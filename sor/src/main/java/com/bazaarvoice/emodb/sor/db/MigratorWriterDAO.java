package com.bazaarvoice.emodb.sor.db;

import com.google.common.util.concurrent.RateLimiter;

import java.util.Iterator;

public interface MigratorWriterDAO {

    void writeRows(String placement, Iterator<MigrationScanResult> results, RateLimiter rateLimiter);
}
