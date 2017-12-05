package com.bazaarvoice.emodb.sor.core;


import com.bazaarvoice.emodb.sor.db.MigrationScanResult;
import com.bazaarvoice.emodb.sor.db.ScanRange;

import java.util.Iterator;

// interface to migrate deltas from old tables to new tables with blocking
public interface MigratorTools {

    void writeRows(String placement, Iterator<MigrationScanResult> results, int maxWritesPerSecond);

    Iterator<MigrationScanResult> readRows(String placement, ScanRange scanRange);
}
