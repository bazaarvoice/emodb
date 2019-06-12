package com.bazaarvoice.emodb.blob.db;

import com.bazaarvoice.emodb.table.db.Table;

import java.nio.ByteBuffer;

/**
 * Chunked storage provider based on the Astyanax {@link com.netflix.astyanax.recipes.storage.ChunkedStorageProvider}
 * interface but with the addition of a {@code timestamp} field so that the metadata and all chunks may be written with
 * the same timestamp.
 * <p>
 * See https://github.com/Netflix/astyanax/wiki/Chunked-Object-Store
 */
public interface StorageProvider {

    long getCurrentTimestamp(Table table);

    void writeChunk(Table table, String blobId, int chunkId, ByteBuffer data, long timestamp);

    ByteBuffer readChunk(Table table, String blobId, int chunkId, long timestamp);

    void deleteObject(Table table, String blobId);

    int getDefaultChunkSize();
}
