package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class MigratorWriterDAO {
    private final DataTools _dataTools;
    private final PlacementCache _placmentCache;

    @Inject
    public MigratorWriterDAO(DataTools dataTools, PlacementCache placementCache) {
        _dataTools = checkNotNull(dataTools, "dataTools");
        _placmentCache = checkNotNull(placementCache, "placementCache");
    }

    public void writeToBlockTable(String placementString, ResultSet resultSet) {
        QueryBuilder.select().all().from("ugc_delta").where(QueryBuilder.gte("rowkey", QueryBuilder.token("token string here")));
    }
}
