package com.bazaarvoice.emodb.sor.core;


import com.bazaarvoice.emodb.sor.db.MigrationScanResultIterator;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.util.Iterator;

public interface MigratorTools {

    public void writeRows(String placement, Iterator<Row> rows);

    public Iterator<MigrationScanResultIterator> readRows(String placement, ScanRange scanRange);
}
