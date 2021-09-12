package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.sor.db.HistoryMigrationScanResult;
import com.bazaarvoice.emodb.sor.db.MigrationScanResult;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import java.util.Iterator;

interface DataCopyWriterDAO {
    void copyDeltasToDestination(Iterator<? extends MigrationScanResult> rows, AstyanaxStorage dest, Runnable progress);

    void copyHistoriesToDestination(Iterator<? extends HistoryMigrationScanResult> rows, AstyanaxStorage dest, Runnable progress);
}
