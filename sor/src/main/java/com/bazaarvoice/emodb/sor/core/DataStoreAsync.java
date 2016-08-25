package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.PurgeStatus;

/**
 * Interface responsible for Async purge jobs for DataStore
 */
public interface DataStoreAsync {
    /**
     * starts a purge job on table
     */
    String purgeTableAsync(String table, Audit audit);

    /**
     * gets the status of the job with jobID
     */
    PurgeStatus getPurgeStatus(String table, String jobID);

}
