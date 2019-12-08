package com.bazaarvoice.emodb.table.db.astyanax;

public interface MaintenanceChecker {
    boolean isTableUnderMaintenance(String table);
}
