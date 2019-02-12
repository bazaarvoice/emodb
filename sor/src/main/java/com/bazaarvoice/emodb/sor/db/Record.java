package com.bazaarvoice.emodb.sor.db;

import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;

import com.bazaarvoice.emodb.sor.db.test.DeltaClusteringKey;
import java.util.Iterator;
import java.util.Map;

/**
 * A record read from the DAO layer.
 */
public interface Record {

    Key getKey();

    Iterator<Map.Entry<DeltaClusteringKey, Compaction>> passOneIterator();

    Iterator<Map.Entry<DeltaClusteringKey, Change>> passTwoIterator();

    Iterator<RecordEntryRawMetadata> rawMetadata();
}
