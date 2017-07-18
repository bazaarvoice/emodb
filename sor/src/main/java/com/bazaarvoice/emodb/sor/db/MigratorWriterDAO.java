package com.bazaarvoice.emodb.sor.db;

import java.util.Iterator;

public interface MigratorWriterDAO {

    public void writeRows(String placement, Iterator<MigrationScanResult> results);
}
