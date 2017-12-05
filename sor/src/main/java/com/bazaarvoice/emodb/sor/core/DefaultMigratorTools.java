package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.db.MigrationScanResult;
import com.bazaarvoice.emodb.sor.db.MigratorReaderDAO;
import com.bazaarvoice.emodb.sor.db.MigratorWriterDAO;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.google.inject.Inject;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultMigratorTools implements MigratorTools {

    private final MigratorReaderDAO _migratorReaderDao;
    private final MigratorWriterDAO _migratorWriterDao;

    @Inject
    public DefaultMigratorTools(MigratorReaderDAO migratorReaderDAO, MigratorWriterDAO migratorWriterDAO) {
        _migratorReaderDao = checkNotNull(migratorReaderDAO, "migratorReaderDao");
        _migratorWriterDao = checkNotNull(migratorWriterDAO, "migratorWriterDao");
    }

    @Override
    public void writeRows(String placement, Iterator<MigrationScanResult> results, int maxWritesPerSecond) {
        checkNotNull(placement, "placement");
        checkNotNull(results, "rows");
        _migratorWriterDao.writeRows(placement, results, maxWritesPerSecond);
    }

    @Override
    public Iterator<MigrationScanResult> readRows(String placement, ScanRange scanRange) {
        checkNotNull(placement, "placement");
        checkNotNull(scanRange, scanRange);
        return _migratorReaderDao.readRows(placement, scanRange);
    }

}
