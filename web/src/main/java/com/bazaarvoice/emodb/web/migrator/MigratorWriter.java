package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.sor.core.MigratorTools;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.io.Closeable;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class MigratorWriter implements Closeable {
    private final MigratorTools _migratorTools;

    private boolean _closed = false;

    @Inject
    public MigratorWriter(MigratorTools migratorTools, @Assisted int taskId, @Assisted String placement) {
        _migratorTools = checkNotNull(migratorTools, "migratorTools");
    }

    public void writeToBlockTable(String placement, ResultSet resultSet) {
        QueryBuilder.select().all().from("ugc_delta").where(QueryBuilder.gte("rowkey", QueryBuilder.token("token string here")));
    }

    @Override
    public void close() throws IOException {
        _closed = true;
    }
}
