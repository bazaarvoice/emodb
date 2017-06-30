package com.bazaarvoice.emodb.sor.db;

import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.datastax.driver.core.ResultSet;

public interface MigratorDAO {
    public void writeRows(String placement, ResultSet resultSet);

    public ResultSet readRows(String placement, ScanRange scanRange);
}
