package com.bazaarvoice.emodb.sor.db;

import com.datastax.driver.core.Row;

import java.util.Iterator;

public interface MigratorWriterDAO {

    public void writeRows(String placement, Iterator<Row> rows);
}
