package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.db.MigrationScanResultIterator;
import com.bazaarvoice.emodb.sor.db.MigratorReaderDAO;
import com.bazaarvoice.emodb.sor.db.MigratorWriterDAO;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
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

    public void writeRows(String placement, Iterator<Row> rows) {
        checkNotNull(placement, "placement");
        checkNotNull(rows, "rows");
        _migratorWriterDao.writeRows(placement, rows);
    }

    public Iterator<MigrationScanResultIterator> readRows(String placement, ScanRange scanRange) {
        checkNotNull(placement, "placement");
        checkNotNull(scanRange, scanRange);
        return _migratorReaderDao.readRows(placement, scanRange);
    }

}
