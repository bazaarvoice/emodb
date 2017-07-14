package com.bazaarvoice.emodb.sor.db;

import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.datastax.driver.core.ResultSet;

import java.util.Iterator;

public interface MigratorReaderDAO {

    public Iterator<MigrationScanResultIterator> readRows(String placement, ScanRange scanRange);
}
