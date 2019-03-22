package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.sor.db.HistoryMigrationScanResult;
import com.bazaarvoice.emodb.sor.db.MigrationScanResult;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import java.util.Iterator;

interface DataCopyReaderDAO {

    Iterator<? extends MigrationScanResult> getDeltasForStorage(AstyanaxStorage source);

    Iterator<? extends HistoryMigrationScanResult> getHistoriesForStorage(AstyanaxStorage source);
}
