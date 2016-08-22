package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;

public interface Placement {

    String getName();

    CassandraKeyspace getKeyspace();
}
