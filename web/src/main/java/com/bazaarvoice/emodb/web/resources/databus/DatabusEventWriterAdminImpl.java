package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.core.DatabusEventWriterRegistry;
import com.bazaarvoice.emodb.sor.core.RecordResolver;
import com.bazaarvoice.emodb.sor.core.Resolved;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.bazaarvoice.emodb.sor.db.DataReaderDAO;
import com.bazaarvoice.emodb.sor.db.DataWriterDAO;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.google.common.collect.Iterators;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DatabusEventWriterAdminImpl implements DatabusEventWriterAdmin {

    private final TableDAO _tableDao;
    private final DataReaderDAO _dataReaderDao;
    private final DataWriterDAO _dataWriterDao;
    private final DatabusEventWriterRegistry _eventWriterRegistry;
    private final RecordResolver _recordResolver;

    public DatabusEventWriterAdminImpl(
            final DatabusEventWriterRegistry eventWriterRegistry,
            final TableDAO tableDao,
            final DataReaderDAO dataReaderDao,
            final DataWriterDAO dataWriterDao,
            final RecordResolver recordResolver) {
        //TODO
        _tableDao = tableDao;
        _dataReaderDao = dataReaderDao;
        _dataWriterDao = dataWriterDao;
        _eventWriterRegistry = eventWriterRegistry;
        _recordResolver = recordResolver;
//        _tableDao = Objects.requireNonNull(tableDao);
//        _dataReaderDao = Objects.requireNonNull(dataReaderDao);
//        _dataWriterDao = Objects.requireNonNull(dataWriterDao);
//        _eventWriterRegistry = Objects.requireNonNull(eventWriterRegistry);
//        _recordResolver = Objects.requireNonNull(recordResolver);
    }

    @Override
    public UpdateRef writeEvent(final String table, final String key, final ReadConsistency consistency, @Nullable final Date date) {
        Objects.requireNonNull(table);
        Objects.requireNonNull(key);
        Objects.requireNonNull(consistency);

        Map<String, Object> document = get(table, key, consistency);
        UpdateRef updateRef = null;
        // send events only if sinceParam isn't specified, or
        // document had been created before sinceParam
        if (getDatePredicate(date).test(document)) {
            long now = System.currentTimeMillis();
            long fct = _dataWriterDao.getFullConsistencyTimestamp(_tableDao.get(table));
            final UUID lastCompactedMutation = (UUID) document.get(RecordResolver.LAST_COMPACTED_MUTATION_KEY);
            final UUID lastCompactionCutoff = (UUID) document.get(RecordResolver.LAST_COMPACTION_CUTOFF_KEY);

            long lastUpdateAt = Intrinsic.getLastUpdateAt(document).getTime();
            UUID changeId = getChangeId(lastUpdateAt, lastCompactedMutation, lastCompactionCutoff, fct, now);
            Set<String> tags = new HashSet<>(Arrays.asList("replay"));

            updateRef = new UpdateRef(table, key, changeId, tags);
            writeEvents(Collections.singleton(updateRef));
        }
        return updateRef;
    }

    private Map<String, Object> get(String tableName, String key, ReadConsistency consistency) {
        Table table = _tableDao.get(tableName);

        // Query from the database
        Record record = _dataReaderDao.read(new Key(table, key), consistency);

        // Resolve the deltas into a single object
        Resolved resolved = _recordResolver.resolve(record, consistency);

        // Convert to the final JSON format including intrinsic fields
        return _recordResolver.toContent(resolved, consistency, true);
    }

    @Override
    public Iterator<UpdateRef> writeEvents(final String table, final int batchSize, final ReadConsistency consistency, @Nullable final Date date) {
        Objects.requireNonNull(table);
        Objects.requireNonNull(consistency);
        //TODO
        return Collections.EMPTY_LIST.iterator();
/*        Collection<String> splits = _dataStore.getSplits(table, batchSize);

        final Iterator<UpdateRef>[] result = new Iterator[] {Iterators.emptyIterator()};
        Predicate<Map<String, Object>> predicate = getDatePredicate(date);
        Set<String> tags = new HashSet<>(Arrays.asList("replay"));

        splits.parallelStream().forEach(split -> {
            Iterator<Map<String, Object>> documents = _dataStore.getSplit(table, split, null, Long.MAX_VALUE, true, consistency);
            Iterable<Map<String, Object>> iterable = () -> documents;
            long now = System.currentTimeMillis();
            long fct = _dataWriterDao.getFullConsistencyTimestamp(_tableDao.get(table));

            List<UpdateRef> updateRefs = StreamSupport.stream(iterable.spliterator(), true)
                    .filter(predicate)
                    .map(document -> {
                        long lastUpdateAt = Intrinsic.getLastUpdateAt(document).getTime();
                        UUID changeId = getChangeId(lastUpdateAt, fct, now);
                        return new UpdateRef(table, Intrinsic.getId(document), changeId, tags);
                    })
                    .collect(Collectors.toList());

            writeEvents(updateRefs);
            result[0] = Iterators.concat(result[0], updateRefs.iterator());
        });
        return result[0];*/
    }

    @Override
    public void writeEvents(final Collection<UpdateRef> refs) {
        _eventWriterRegistry.getDatabusWriter().writeEvents(refs);
    }

    private static Predicate<Map<String, Object>> getDatePredicate(Date date) {
        Predicate<Map<String, Object>> datePredicate = document -> Intrinsic.getFirstUpdateAt(document).compareTo(date) >= 0;
        Predicate<Map<String, Object>> alwaysTruePredicate = document -> true;
        return date == null ? alwaysTruePredicate : datePredicate;
    }

    private static UUID getChangeId(long lastUpdateAt, final UUID lastCompactedMutation, final UUID lastCompactionCutoff, long fct, long now) {
        //TODO
        return TimeUUIDs.uuidForTimestamp(getDateBetween(Math.max(fct, lastUpdateAt), now));
    }

    private static Date getDateBetween(long from, long to) {
        return new Date(ThreadLocalRandom.current().nextLong(from, to));
    }

}
