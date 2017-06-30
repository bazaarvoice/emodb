package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.db.MigratorDAO;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.datastax.driver.core.ResultSet;
import com.google.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultMigratorTools implements MigratorTools {

    private final MigratorDAO _migratorDao;

    @Inject
    public DefaultMigratorTools(MigratorDAO migratorDAO) {
        _migratorDao = checkNotNull(migratorDAO, "migratorDao");
    }

    public void writeRows(String placement, ResultSet resultSet) {
        checkNotNull(resultSet, "resultSet");
        _migratorDao.writeRows(placement, resultSet);
    }

    public ResultSet readRows(String placement, ScanRange scanRange) {
        checkNotNull(placement, "placement");
        checkNotNull(scanRange, scanRange);
        return _migratorDao.readRows(placement, scanRange);
    }

}
