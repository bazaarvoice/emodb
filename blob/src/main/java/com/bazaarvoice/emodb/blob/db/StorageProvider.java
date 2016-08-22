package com.bazaarvoice.emodb.blob.db;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.table.db.Table;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

/**
 * Chunked storage provider based on the Astyanax {@link com.netflix.astyanax.recipes.storage.ChunkedStorageProvider}
 * interface but with the addition of a {@code timestamp} field so that the metadata and all chunks may be written with
 * the same timestamp.
 * <p>
 * See https://github.com/Netflix/astyanax/wiki/Chunked-Object-Store
 */
public interface StorageProvider {

    long getCurrentTimestamp(Table table);

    void writeChunk(Table table, String blobId, int chunkId, ByteBuffer data, @Nullable Duration ttl, long timestamp);

    ByteBuffer readChunk(Table table, String blobId, int chunkId, long timestamp);

    void deleteObject(Table table, String blobId, Integer chunkCount);

    void writeMetadata(Table table, String blobId, StorageSummary summary, @Nullable Duration ttl);

    StorageSummary readMetadata(Table table, String blobId);

    Iterator<Map.Entry<String, StorageSummary>> scanMetadata(Table table, @Nullable String fromBlobIdExclusive, LimitCounter limit);

    long count(Table table);

    void purge(Table table);

    int getDefaultChunkSize();
}
