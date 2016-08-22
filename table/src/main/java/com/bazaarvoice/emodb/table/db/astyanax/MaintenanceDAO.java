package com.bazaarvoice.emodb.table.db.astyanax;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;

public interface MaintenanceDAO {
    Iterator<Map.Entry<String, MaintenanceOp>> listMaintenanceOps();

    @Nullable
    MaintenanceOp getNextMaintenanceOp(String table);

    void performMetadataMaintenance(String table);

    void performDataMaintenance(String table, Runnable progress);
}
