package com.bazaarvoice.emodb.sor.core;


import com.bazaarvoice.emodb.sor.api.Compaction;

public class RowVersionUtils {
    /**
     * Find the version of the first delta of a pending compaction retroactively
     */
    public static long getVersionRetroactively(PendingCompaction pendingCompaction) {
        Compaction compaction = pendingCompaction.getCompaction();
        // Calculate the version and create a dummy compaction record for the resolver
        return compaction.getCount() - (long)pendingCompaction.getDeltasToArchive().size();
    }
}
