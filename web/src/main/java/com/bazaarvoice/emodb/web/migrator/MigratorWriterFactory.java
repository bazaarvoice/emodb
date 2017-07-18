package com.bazaarvoice.emodb.web.migrator;

/**
 * Interface used to create migration writer implementations
 */

public interface MigratorWriterFactory {
    MigratorWriter createMigratorWriter(int taskId, String placement);
}
