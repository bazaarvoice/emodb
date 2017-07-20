package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.sor.core.MigratorTools;
import com.bazaarvoice.emodb.sor.db.MigrationScanResult;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class MigratorWriter implements Closeable {
    private final MigratorTools _migratorTools;

    private volatile boolean _closed = false;

    @Inject
    public MigratorWriter(MigratorTools migratorTools, @Assisted int taskId, @Assisted String placement) {
        _migratorTools = checkNotNull(migratorTools, "migratorTools");
    }

    public void writeToBlockTable(String placement, List<MigrationScanResult> results) throws IOException {
        if (_closed) {
            throw new IOException("Writer closed");
        }

        _migratorTools.writeRows(placement, results.iterator());
    }

    @Override
    public void close() throws IOException {
        _closed = true;
    }
}
