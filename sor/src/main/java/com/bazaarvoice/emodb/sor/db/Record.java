package com.bazaarvoice.emodb.sor.db;

import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * A record read from the DAO layer.
 */
public interface Record {

    Key getKey();

    Iterator<Map.Entry<UUID, Compaction>> passOneIterator();

    Iterator<Map.Entry<UUID, Change>> passTwoIterator();

    Iterator<RecordEntryRawMetadata> rawMetadata();
}
