package com.bazaarvoice.emodb.blob.client;

import com.bazaarvoice.emodb.blob.api.BlobMetadata;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.Table;
import com.bazaarvoice.emodb.common.json.RestartingStreamingIterator;
import com.bazaarvoice.emodb.common.json.StreamingIteratorSupplier;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Wraps the streaming APIs in the {@link BlobStore} with logic that automatically restarts the iterator in the
 * event that the connection to the EmoDB server is lost while streaming results.  This complements the Ostrich
 * retry logic and extends it to support long-lived streaming connections to EmoDB.
 */
public abstract class BlobStoreStreaming {

    /**
     * Prevent instantiation.
     */
    private BlobStoreStreaming() {
    }

    public static Iterable<Table> listTables(BlobStore blobStore) {
        return listTables(blobStore, null, Long.MAX_VALUE);
    }

    public static Iterable<BlobMetadata> scanMetadata(BlobStore blobStore, String table) {
        return scanMetadata(blobStore, table, null, Long.MAX_VALUE);
    }

    public static Iterable<Table> listTables(final BlobStore blobStore,
                                             final @Nullable String fromTableExclusive,
                                             final long limit) {
        return RestartingStreamingIterator.stream(fromTableExclusive, limit,
                new StreamingIteratorSupplier<Table, String>() {
                    @Override
                    public Iterator<Table> get(String fromToken, long limit) {
                        return blobStore.listTables(fromToken, limit);
                    }

                    @Override
                    public String getNextToken(Table table) {
                        return table.getName();
                    }
                });
    }

    public static Iterable<BlobMetadata> scanMetadata(final BlobStore blobStore,
                                                      final String table,
                                                      final @Nullable String fromKeyExclusive,
                                                      final long limit) {
        return RestartingStreamingIterator.stream(fromKeyExclusive, limit,
                new StreamingIteratorSupplier<BlobMetadata, String>() {
                    @Override
                    public Iterator<BlobMetadata> get(String fromToken, long limit) {
                        return blobStore.scanMetadata(table, fromToken, limit);
                    }

                    @Override
                    public String getNextToken(BlobMetadata object) {
                        return object.getId();
                    }
                });
    }
}
