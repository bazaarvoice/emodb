package com.bazaarvoice.emodb.blob.db;

import com.bazaarvoice.emodb.blob.api.Range;
import com.bazaarvoice.emodb.blob.api.StreamSupplier;
import com.bazaarvoice.emodb.table.db.Table;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.util.Map;

/**
 * Chunked storage provider based on the Astyanax {@link com.netflix.astyanax.recipes.storage.ChunkedStorageProvider}
 * interface but with the addition of a {@code timestamp} field so that the metadata and all chunks may be written with
 * the same timestamp.
 * <p>
 * See https://github.com/Netflix/astyanax/wiki/Chunked-Object-Store
 */
public interface StorageProvider {

    StorageSummary putObject(final Table table, final String blobId, final InputStream input, final Map<String, String> attributes);

    StreamSupplier getObjectStreamSupplier(Table table, String blobId, StorageSummary summary, @Nullable Range range);

    void deleteObject(Table table, String blobId);

}
