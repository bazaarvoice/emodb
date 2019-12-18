package com.bazaarvoice.emodb.table.db.eventregistry;

import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;

import java.util.stream.Stream;

public interface StorageReaderDAO {
    Stream<String> getIdsForStorage(AstyanaxStorage storage);
}
