package com.bazaarvoice.emodb.table.db.astyanax;

public interface FullConsistencyTimeProvider {
    /** Returns a timestamp that is the latest possible time that everything was consistent. */
    long getMaxTimeStamp(String cluster);
}
