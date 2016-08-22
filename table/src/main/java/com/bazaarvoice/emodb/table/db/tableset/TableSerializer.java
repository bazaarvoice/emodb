package com.bazaarvoice.emodb.table.db.tableset;

import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.table.db.DroppedTableException;
import com.bazaarvoice.emodb.table.db.Table;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

/**
 * Defines the methods for loading, serializing and deserializing table metadata for use in a TableSet.
 */
public interface TableSerializer {

    /**
     * Writes a representation of this table to the given output stream.
     *
     * @return The set of UUIDs associated with this table.
     *
     */
    Set<Long> loadAndSerialize(long uuid, OutputStream out)
            throws IOException, UnknownTableException, DroppedTableException;

    /**
     * Reads a representation previously written in {@link #loadAndSerialize(long, java.io.OutputStream)}
     * and returns it as a Table.
     */
    Table deserialize(InputStream in)
            throws IOException;
}
