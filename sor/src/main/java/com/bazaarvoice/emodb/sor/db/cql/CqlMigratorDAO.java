package com.bazaarvoice.emodb.sor.db.cql;


import com.bazaarvoice.emodb.common.cassandra.CqlDriverConfiguration;
import com.bazaarvoice.emodb.sor.db.MigratorDAO;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.db.astyanax.ChangeEncoder;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.datastax.driver.core.ResultSet;
import com.google.inject.Inject;

public class CqlMigratorDAO implements MigratorDAO {

    private final PlacementCache _placementCache;
    private final ChangeEncoder _changeEncoder;
    private final CqlDriverConfiguration _driverConfig;

    @Inject
    public CqlMigratorDAO(PlacementCache placementCache, ChangeEncoder changeEncoder, CqlDriverConfiguration driverConfig) {
        _placementCache = placementCache;
        _changeEncoder = changeEncoder;
        _driverConfig = driverConfig;
    }

    @Override
    public void writeRows(String placement, ResultSet resultSet) {

    }

    @Override
    public ResultSet readRows(String placement, ScanRange scanRange) {
        return null;
    }
}
