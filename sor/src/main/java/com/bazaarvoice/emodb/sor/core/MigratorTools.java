package com.bazaarvoice.emodb.sor.core;


import com.bazaarvoice.emodb.sor.db.MigrationScanResult;
import com.bazaarvoice.emodb.sor.db.ScanRange;

import java.util.Iterator;

public interface MigratorTools {

    public void writeRows(String placement, Iterator<MigrationScanResult> results);

    public Iterator<MigrationScanResult> readRows(String placement, ScanRange scanRange);
}
