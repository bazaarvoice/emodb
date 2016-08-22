package com.bazaarvoice.emodb.table.db.astyanax;

public interface DataCopyDAO {
    void copy(AstyanaxStorage source, AstyanaxStorage dest, Runnable progress);
}
