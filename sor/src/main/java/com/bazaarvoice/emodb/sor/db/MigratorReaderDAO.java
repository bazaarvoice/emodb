package com.bazaarvoice.emodb.sor.db;

import java.util.Iterator;

public interface MigratorReaderDAO {

    public Iterator<MigrationScanResult> readRows(String placement, ScanRange scanRange);
}
