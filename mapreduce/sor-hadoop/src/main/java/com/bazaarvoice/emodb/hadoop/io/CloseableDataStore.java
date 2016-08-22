package com.bazaarvoice.emodb.hadoop.io;

import com.bazaarvoice.emodb.sor.api.DataStore;

import java.io.Closeable;

/**
 * The base DataStore interface is does not require closing.  However, the implementation used here may require closing
 * to perform ZooKeeper and connection cleanup.  This interface extends DataStore to allow for closing when it is
 * no longer required.
 */
public interface CloseableDataStore extends DataStore, Closeable {
}
