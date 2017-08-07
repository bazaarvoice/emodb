package com.bazaarvoice.emodb.sor.db;

import java.util.Iterator;

public interface MigratorWriterDAO {

    void writeRows(String placement, Iterator<MigrationScanResult> results, int maxConcurrentWrites);
}
