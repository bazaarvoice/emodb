package com.bazaarvoice.emodb.sor.db.cql;

import com.datastax.driver.core.Row;

import java.util.Iterator;

public interface RowGroup extends Iterator<Row> {

    Iterator<Row> reloadRowsAfter(Row row);
}
