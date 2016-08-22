package com.bazaarvoice.emodb.table.db.astyanax;

public interface MaintenanceTask {
    /**
     * Perform maintenance.  The {@code progress} runnable should be invoked on a regular basis if
     * this maintenance takes a while.
     */
    void run(Runnable progress);
}
