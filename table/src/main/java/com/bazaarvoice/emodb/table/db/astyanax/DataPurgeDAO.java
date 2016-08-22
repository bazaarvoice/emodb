package com.bazaarvoice.emodb.table.db.astyanax;

public interface DataPurgeDAO {
    void purge(AstyanaxStorage storage, Runnable progress);
}
