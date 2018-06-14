package com.bazaarvoice.emodb.sor.core.test;

import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.ChangeBuilder;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.core.HistoryBatchPersister;
import com.bazaarvoice.emodb.sor.core.HistoryStore;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;

public class InMemoryHistoryStore implements HistoryStore {
    private final ConcurrentMap<String, List<History>> _historyStore = Maps.newConcurrentMap();

    @Override
    public Iterator<Change> getDeltaHistories(String table, String rowId) {
        String key = getKey(table, rowId);
        if (!_historyStore.containsKey(key)) {
            return Iterators.emptyIterator();
        }
        return Lists.transform(_historyStore.get(key), new Function<History, Change>() {
            @Override
            public Change apply(History input) {
                return new ChangeBuilder(input.getChangeId())
                        .with(input).with(input.getDelta()).build();
            }
        }).iterator();
    }

    @Override
    public void putDeltaHistory(String table, String rowId, List<History> deltaHistories) {
        // Protection against immutable lists passed into the method
        List<History> audits = Lists.newArrayList(deltaHistories);
        List<History> historyList = _historyStore.putIfAbsent(getKey(table, rowId), audits);
        if (historyList != null) { // already had some deltas
            historyList.addAll(audits);
            _historyStore.put(getKey(table, rowId), historyList);
        }
    }

    @Override
    public void putDeltaHistory(Object rowId, List<History> deltaAudits, HistoryBatchPersister historyBatchPersister) {
        throw new UnsupportedOperationException("Batch persistor is not supported for in-memory HistoryStore");
    }

    @Override
    public Duration getHistoryTtl() {
        return Duration.ofDays(365); // No ttl for in-memory
    }

    @Override
    public boolean isDeltaHistoryEnabled() {
        return !Duration.ZERO.equals(getHistoryTtl());
    }

    private String getKey(String table, String rowId) {
        return format("%s/%s", table, rowId);
    }
}
