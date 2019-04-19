package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.core.DatabusEventWriter;
import com.bazaarvoice.emodb.sor.core.UpdateRef;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Iterator;

/**
 * Interface that should write events to the databus prior to writing them to the system of record.
 */
public interface DatabusEventWriterAdmin extends DatabusEventWriter {

    /**
     * Write event to databus for specified table and key.
     *
     * @param table       the table
     * @param key         the key
     * @param consistency the consistency
     * @param date        the date
     * @return the update ref
     */
    UpdateRef writeEvent(String table, final String key, final ReadConsistency consistency, @Nullable final Date date);

    /**
     * Write events to databus for specified table.
     *
     * @param table       the table
     * @param batchSize   the batch size
     * @param consistency the consistency
     * @param date        the date
     * @return the iterator
     */
    Iterator<UpdateRef> writeEvents(final String table, int batchSize, final ReadConsistency consistency, @Nullable final Date date);

}
