package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.db.RecordEntryRawMetadata;

import com.bazaarvoice.emodb.sor.db.test.DeltaClusteringKey;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

class RecordImpl implements Record {
    private final Key _key;
    private Iterator<Map.Entry<DeltaClusteringKey, Compaction>> _passOneIterator;
    private Iterator<Map.Entry<DeltaClusteringKey, Change>> _passTwoIterator;
    private Iterator<RecordEntryRawMetadata> _rawMetadataIterator;

    RecordImpl(Key key,
               Iterator<Map.Entry<DeltaClusteringKey, Compaction>> passOneIterator,
               Iterator<Map.Entry<DeltaClusteringKey, Change>> passTwoIterator,
               Iterator<RecordEntryRawMetadata> rawMetadataIterator) {
        _key = key;
        _passOneIterator = passOneIterator;
        _passTwoIterator = passTwoIterator;
        _rawMetadataIterator = rawMetadataIterator;
    }

    @Override
    public Key getKey() {
        return _key;
    }

    @Override
    public Iterator<Map.Entry<DeltaClusteringKey, Compaction>> passOneIterator() {
        Iterator<Map.Entry<DeltaClusteringKey, Compaction>> result = checkNotNull(_passOneIterator, "Already consumed.");
        _passOneIterator = null;
        return result;
    }

    @Override
    public Iterator<Map.Entry<DeltaClusteringKey, Change>> passTwoIterator() {
        Iterator<Map.Entry<DeltaClusteringKey, Change>> result = checkNotNull(_passTwoIterator, "Already consumed.");
        _passTwoIterator = null;
        return result;
    }

    @Override
    public Iterator<RecordEntryRawMetadata> rawMetadata() {
        Iterator<RecordEntryRawMetadata> result = checkNotNull(_rawMetadataIterator, "Already consumed.");
        _rawMetadataIterator = null;
        return result;
    }

    @Override
    public String toString() {
        return _key.toString(); // for debugging
    }
}
