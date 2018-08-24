package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.json.deferred.LazyJsonMap;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.DefaultTable;
import com.bazaarvoice.emodb.sor.api.FacadeOptions;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.Names;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.StashNotAvailableException;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.bazaarvoice.emodb.sor.api.StashTimeKey;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.api.UnpublishedDatabusEvent;
import com.bazaarvoice.emodb.sor.api.UnpublishedDatabusEventType;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.compactioncontrol.LocalCompactionControl;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.db.DataReaderDAO;
import com.bazaarvoice.emodb.sor.db.DataWriterDAO;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.MultiTableScanOptions;
import com.bazaarvoice.emodb.sor.db.MultiTableScanResult;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.db.RecordUpdate;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.db.ScanRangeSplits;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.log.SlowQueryLog;
import com.bazaarvoice.emodb.table.db.DroppedTableException;
import com.bazaarvoice.emodb.table.db.StashBlackListTableCondition;
import com.bazaarvoice.emodb.table.db.StashTableDAO;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableBackingStore;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.bazaarvoice.emodb.table.db.stash.StashTokenRange;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.ExecutorServiceManager;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class DefaultDataStore implements DataStore, DataProvider, DataTools, TableBackingStore {

    private static final int NUM_COMPACTION_THREADS = 2;
    private static final int MAX_COMPACTION_QUEUE_LENGTH = 100;

    private final EventBus _eventBus;
    private final TableDAO _tableDao;
    private final DataReaderDAO _dataReaderDao;
    private final DataWriterDAO _dataWriterDao;
    private final SlowQueryLog _slowQueryLog;
    private final ExecutorService _compactionExecutor;
    private final AuditStore _auditStore;
    private final Optional<URI> _stashRootDirectory;
    private final Condition _stashBlackListTableCondition;
    private final Timer _resolveAnnotatedEventTimer;
    @VisibleForTesting
    protected final Counter _archiveDeltaSize;
    private final Meter _discardedCompactions;
    private final Compactor _compactor;
    private final CompactionControlSource _compactionControlSource;

    private StashTableDAO _stashTableDao;

    @Inject
    public DefaultDataStore(LifeCycleRegistry lifeCycle, MetricRegistry metricRegistry, EventBus eventBus, TableDAO tableDao,
                            DataReaderDAO dataReaderDao, DataWriterDAO dataWriterDao, SlowQueryLog slowQueryLog, AuditStore auditStore,
                            @StashRoot Optional<URI> stashRootDirectory, @LocalCompactionControl CompactionControlSource compactionControlSource,
                            @StashBlackListTableCondition Condition stashBlackListTableCondition) {
        this(eventBus, tableDao, dataReaderDao, dataWriterDao, slowQueryLog, defaultCompactionExecutor(lifeCycle),
                auditStore, stashRootDirectory, compactionControlSource, stashBlackListTableCondition, metricRegistry);
    }

    @VisibleForTesting
    public DefaultDataStore(EventBus eventBus, TableDAO tableDao, DataReaderDAO dataReaderDao, DataWriterDAO dataWriterDao,
                            SlowQueryLog slowQueryLog, ExecutorService compactionExecutor, AuditStore auditStore,
                            Optional<URI> stashRootDirectory, CompactionControlSource compactionControlSource, Condition stashBlackListTableCondition, MetricRegistry metricRegistry) {
        _eventBus = checkNotNull(eventBus, "eventBus");
        _tableDao = checkNotNull(tableDao, "tableDao");
        _dataReaderDao = checkNotNull(dataReaderDao, "dataReaderDao");
        _dataWriterDao = checkNotNull(dataWriterDao, "dataWriterDao");
        _slowQueryLog = checkNotNull(slowQueryLog, "slowQueryLog");
        _compactionExecutor = checkNotNull(compactionExecutor, "compactionExecutor");
        _auditStore = checkNotNull(auditStore, "auditStore");
        _stashRootDirectory = checkNotNull(stashRootDirectory, "stashRootDirectory");
        _stashBlackListTableCondition = checkNotNull(stashBlackListTableCondition, "stashBlackListTableCondition");
        _resolveAnnotatedEventTimer = metricRegistry.timer(getMetricName("resolve_event"));

        _archiveDeltaSize = metricRegistry.counter(MetricRegistry.name("bv.emodb.sor", "DefaultCompactor", "archivedDeltaSize"));
        _discardedCompactions = metricRegistry.meter(MetricRegistry.name("bv.emodb.sor", "DefaultDataStore", "discarded_compactions"));
        _compactor = new DistributedCompactor(_archiveDeltaSize, _auditStore.isDeltaHistoryEnabled(), metricRegistry);

        _compactionControlSource = checkNotNull(compactionControlSource, "compactionControlSource");
    }

    /**
     * Optional binding, required only if running in stash mode.
     */
    @Inject (optional = true)
    public void setStashDAO(StashTableDAO stashTableDao) {
        _stashTableDao = stashTableDao;
    }

    private static ExecutorService defaultCompactionExecutor(LifeCycleRegistry lifeCycle) {
        String nameFormat = "DataStore Compaction-%d";
        ExecutorService executor = new ThreadPoolExecutor(
                NUM_COMPACTION_THREADS, NUM_COMPACTION_THREADS,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(MAX_COMPACTION_QUEUE_LENGTH),
                new ThreadFactoryBuilder().setNameFormat(nameFormat).build(),
                new ThreadPoolExecutor.AbortPolicy());
        lifeCycle.manage(new ExecutorServiceManager(executor, io.dropwizard.util.Duration.seconds(5), nameFormat));
        return executor;
    }

    @Override
    public Iterator<com.bazaarvoice.emodb.sor.api.Table> listTables(@Nullable String fromTableExclusive, long limit) {
        checkArgument(limit > 0, "Limit must be >0");

        LimitCounter remaining = new LimitCounter(limit);
        final Iterator<Table> tableIter = _tableDao.list(fromTableExclusive, remaining);
        return remaining.limit(new AbstractIterator<com.bazaarvoice.emodb.sor.api.Table>() {
            @Override
            protected com.bazaarvoice.emodb.sor.api.Table computeNext() {
                while (tableIter.hasNext()) {
                    Table table = tableIter.next();
                    if (!table.isInternal()) {
                        return toDefaultTable(table);
                    }
                }
                return endOfData();
            }
        });
    }

    @Override
    public Iterator<UnpublishedDatabusEvent> listUnpublishedDatabusEvents(Date fromInclusive, Date toExclusive) {
        return _tableDao.listUnpublishedDatabusEvents(fromInclusive, toExclusive);
    }

    private DefaultTable toDefaultTable(Table table) {
        return new DefaultTable(table.getName(), table.getOptions(), table.getAttributes(), table.getAvailability());
    }

    @Override
    public void createTable(String table, TableOptions options, Map<String, ?> template, Audit audit) {
        checkLegalTableName(table);
        checkNotNull(options, "options");
        checkLegalTableAttributes(template);
        checkNotNull(audit, "audit");
        _tableDao.create(table, options, template, audit);
    }

    @Override
    public void dropTable(String table, Audit audit) {
        checkLegalTableName(table);
        checkNotNull(audit, "audit");
        _tableDao.drop(table, audit);
    }

    @Override
    public void purgeTableUnsafe(String tableName, Audit audit) {
        checkLegalTableName(tableName);
        checkNotNull(audit, "audit");
        Table table = _tableDao.get(tableName);
        _tableDao.writeUnpublishedDatabusEvent(tableName, UnpublishedDatabusEventType.PURGE);
        _tableDao.audit(tableName, "purge", audit);
        _dataWriterDao.purgeUnsafe(table);
    }

    @Override
    public boolean getTableExists(String table) {
        checkLegalTableName(table);
        return _tableDao.exists(table);
    }

    @Override
    public boolean isTableAvailable(String table) {
        checkLegalTableName(table);
        return _tableDao.get(table).getAvailability() != null;
    }

    @Override
    public com.bazaarvoice.emodb.sor.api.Table getTableMetadata(String table) {
        checkLegalTableName(table);
        return toDefaultTable(_tableDao.get(table));
    }

    @Override
    public Table getTable(String table) {
        return _tableDao.get(table);
    }

    @Override
    public Map<String, Object> getTableTemplate(String table) {
        checkLegalTableName(table);
        return _tableDao.get(table).getAttributes();
    }

    @Override
    public void setTableTemplate(String table, Map<String, ?> template, Audit audit)
            throws UnknownTableException {
        checkLegalTableName(table);
        checkLegalTableAttributes(template);
        checkNotNull(audit, "audit");
        _tableDao.setAttributes(table, template, audit);
    }

    @Override
    public TableOptions getTableOptions(String table) {
        checkLegalTableName(table);
        return _tableDao.get(table).getOptions();
    }

    @Override
    public long getTableApproximateSize(String tableName) {
        checkLegalTableName(tableName);

        Table table = _tableDao.get(tableName);
        return _dataReaderDao.count(table, ReadConsistency.WEAK);
    }

    @Override
    public long getTableApproximateSize(String tableName, int limit) {
        checkLegalTableName(tableName);
        checkNotNull(limit);
        checkArgument(limit > 0, "limit must be greater than 0");

        Table table = _tableDao.get(tableName);
        return _dataReaderDao.count(table, limit, ReadConsistency.WEAK);
    }

    @Override
    public Map<String, Object> get(String table, String key) {
        return get(table, key, ReadConsistency.STRONG);
    }

    @Override
    public Map<String, Object> get(String tableName, String key, ReadConsistency consistency) {
        checkLegalTableName(tableName);
        checkNotNull(key, "key");
        checkNotNull(consistency, "consistency");

        Table table = _tableDao.get(tableName);

        // Query from the database
        Record record = _dataReaderDao.read(new Key(table, key), consistency);

        // Resolve the deltas into a single object
        Resolved resolved = resolve(record, consistency);

        // Convert to the final JSON format including intrinsic fields
        return toContent(resolved, consistency);
    }

    @Override
    public AnnotatedGet prepareGetAnnotated(final ReadConsistency consistency) {
        checkNotNull(consistency, "consistency");

        return new AnnotatedGet() {
            private final List<Key> _keys = Lists.newArrayList();

            @Override
            public AnnotatedGet add(String tableName, String key)
                    throws UnknownTableException, UnknownPlacementException {
                checkLegalTableName(tableName);
                checkNotNull(key, "key");

                // The following call will throw UnknownTableException if the table doesn't currently exist.  This
                // happens if the table was dropped prior to this call.
                Table table = _tableDao.get(tableName);
                // It's also possible that the table exists but is not available locally.  This happens if the table
                // once had a locally available facade but the facade was since dropped.  Check if this is the case and,
                // if so, raise UnknownPlacementException.
                if (table.getAvailability() == null) {
                    throw new UnknownPlacementException("Table unavailable locally and has no local facade",
                            table.getOptions().getPlacement(), tableName);
                }
                _keys.add(new Key(table, key));

                return this;
            }

            @Override
            public Iterator<AnnotatedContent> execute() {
                if (_keys.isEmpty()) {
                    return Iterators.emptyIterator();
                }
                // Limit memory usage using an iterator such that only one row's change list is in memory at a time.
                Iterator<Record> recordIterator = _dataReaderDao.readAll(_keys, consistency);
                return Iterators.transform(recordIterator, new Function<Record, AnnotatedContent>() {
                    @Override
                    public AnnotatedContent apply(Record record) {
                        Timer.Context timerCtx = _resolveAnnotatedEventTimer.time();
                        AnnotatedContent result = resolveAnnotated(record, consistency);
                        timerCtx.stop();
                        return result;
                    }
                });
            }
        };
    }

    /**
     * Resolve a set of changes read from the {@link DataWriterDAO} into a single JSON literal object + metadata.
     * If the record can be compacted an asynchronous compaction will be scheduled unless
     * scheduleCompactionIfPresent is false.
     */
    private Resolved resolve(Record record, ReadConsistency consistency, boolean scheduleCompactionIfPresent) {
        Table table = record.getKey().getTable();
        String key = record.getKey().getKey();

        // Resolve the timeline into a flattened object
        Expanded expanded = expand(record, false, consistency);

        // Log records with too many uncompacted deltas as a potential performance problem.
        _slowQueryLog.log(table.getName(), key, expanded);

        // Are there deltas in this record that we no longer need?  If so, schedule an asynchronous compaction.
        if (scheduleCompactionIfPresent && expanded.getPendingCompaction() != null) {
            compactAsync(table, key, expanded.getPendingCompaction());
        }

        return expanded.getResolved();
    }

    /**
     * Convenience call to {@link #resolve(Record, ReadConsistency, boolean)} which always schedules asynchronous
     * compaction is applicable.
     */
    private Resolved resolve(Record record, ReadConsistency consistency) {
        return resolve(record, consistency, true);
    }

    private Expanded expand(final Record record, boolean ignoreRecent, final ReadConsistency consistency) {
        long fullConsistencyTimeStamp = _dataWriterDao.getFullConsistencyTimestamp(record.getKey().getTable());
        long rawConsistencyTimeStamp = _dataWriterDao.getRawConsistencyTimestamp(record.getKey().getTable());
        Map<StashTimeKey, StashRunTimeInfo> stashTimeInfoMap = _compactionControlSource.getStashTimesForPlacement(record.getKey().getTable().getAvailability().getPlacement());
        // we will consider the earliest timestamp found as our compactionControlTimestamp.
        // we are also filtering out any expired timestamps. (CompactionControlMonitor should do this for us, but for now it's running every hour. So, just to fill that gap, we are filtering here.)
        // If no timestamps are found, then taking minimum value because we want all the deltas after the compactionControlTimestamp to be deleted as per the compaction rules as usual.
        long compactionControlTimestamp = stashTimeInfoMap.isEmpty() ?
                Long.MIN_VALUE : stashTimeInfoMap.values().stream().filter(s -> s.getExpiredTimestamp() > System.currentTimeMillis()).map(StashRunTimeInfo::getTimestamp).min(Long::compareTo).orElse(Long.MIN_VALUE);
        return expand(record, fullConsistencyTimeStamp, rawConsistencyTimeStamp, compactionControlTimestamp, ignoreRecent, consistency);
    }

    private Expanded expand(final Record record, long fullConsistencyTimestamp, long rawConsistencyTimestamp, long compactionControlTimestamp, boolean ignoreRecent, final ReadConsistency consistency) {
        MutableIntrinsics intrinsics = MutableIntrinsics.create(record.getKey());
        return _compactor.expand(record, fullConsistencyTimestamp, rawConsistencyTimestamp, compactionControlTimestamp, intrinsics, ignoreRecent, new Supplier<Record>() {
            @Override
            public Record get() {
                return _dataReaderDao.read(record.getKey(), consistency);
            }
        });
    }

    /**
     * Resolve a set of changes, returning an interface that includes info about specific change IDs.
     */
    private AnnotatedContent resolveAnnotated(Record record, final ReadConsistency consistency) {
        final Resolved resolved = resolve(record, consistency);

        final Table table = record.getKey().getTable();
        return new AnnotatedContent() {
            @Override
            public Map<String, Object> getContent() {
                return toContent(resolved, consistency);
            }

            @Override
            public boolean isChangeDeltaPending(UUID changeId) {
                long fullConsistencyTimestamp = _dataWriterDao.getFullConsistencyTimestamp(table);
                return resolved.isChangeDeltaPending(changeId, fullConsistencyTimestamp);
            }

            @Override
            public boolean isChangeDeltaRedundant(UUID changeId) {
                return resolved.isChangeDeltaRedundant(changeId);
            }
        };
    }

    @VisibleForTesting
    public Map<String, Object> toContent(Resolved resolved, ReadConsistency consistency) {
        Map<String, Object> result;
        if (!resolved.isUndefined()) {
            Object content = resolved.getContent();
            if (content instanceof LazyJsonMap) {
                // If the content is a lazy map it's more efficient to create a copy.
                result = ((LazyJsonMap) content).lazyCopy();
            } else {
                result = Maps.newLinkedHashMap();
                if (content instanceof Map) {
                    for (Map.Entry<?, ?> entry : ((Map<?, ?>) content).entrySet()) {
                        result.put(entry.getKey().toString(), entry.getValue());
                    }
                }
            }
        } else {
            result = Maps.newLinkedHashMap();
        }

        MutableIntrinsics intrinsics = resolved.getIntrinsics();
        result.putAll(intrinsics.getTemplate());
        result.put(Intrinsic.ID, checkNotNull(intrinsics.getId()));
        result.put(Intrinsic.TABLE, checkNotNull(intrinsics.getTable()));
        if (consistency != ReadConsistency.WEAK) {
            // Version #s are consistent within a data center when reads are performed using LOCAL_QUORUM. this
            // means (a) you can't compare version #s from different data centers (unless you read w/EACH_QUORUM,
            // which we don't) and (b) version #s can't be trusted with anything weaker than LOCAL_QUORUM.
            result.put(Intrinsic.VERSION, intrinsics.getVersion());
        }
        result.put(Intrinsic.SIGNATURE, checkNotNull(intrinsics.getSignature()));
        result.put(Intrinsic.DELETED, resolved.isUndefined());
        // Note that Dates are formatted as strings not Date objects so the result is standard JSON
        String firstUpdateAt = intrinsics.getFirstUpdateAt();
        if (firstUpdateAt != null) {
            result.put(Intrinsic.FIRST_UPDATE_AT, firstUpdateAt);
        }
        String lastUpdateAt = intrinsics.getLastUpdateAt();
        if (lastUpdateAt != null) {
            result.put(Intrinsic.LAST_UPDATE_AT, lastUpdateAt);
        }
        String lastMutateAt = intrinsics.getLastMutateAt();
        if (lastMutateAt != null) {
            result.put(Intrinsic.LAST_MUTATE_AT, lastMutateAt);
        }
        return result;
    }

    @Override
    public Iterator<Change> getTimeline(String tableName, String key, boolean includeContentData, boolean includeAuditInformation,
                                        @Nullable UUID start, @Nullable UUID end, boolean reversed, long limit, ReadConsistency consistency) {
        checkLegalTableName(tableName);
        checkNotNull(key, "key");
        if (start != null && end != null) {
            if (reversed) {
                checkArgument(TimeUUIDs.compare(start, end) >= 0, "Start must be >=End for reversed ranges");
            } else {
                checkArgument(TimeUUIDs.compare(start, end) <= 0, "Start must be <=End");
            }
        }
        checkArgument(limit > 0, "Limit must be >0");
        checkNotNull(consistency, "consistency");

        Table table = _tableDao.get(tableName);

        // Query the database.  Return the raw timeline.  Don't perform compaction--it modifies the timeline which
        // could make getTimeline() less useful for debugging.
        return _dataReaderDao.readTimeline(new Key(table, key), includeContentData, includeAuditInformation, start, end, reversed, limit, consistency);
    }

    @Override
    public Iterator<Map<String, Object>> scan(String tableName, @Nullable String fromKeyExclusive,
                                              long limit, boolean includeDeletes, ReadConsistency consistency) {
        checkLegalTableName(tableName);
        checkArgument(limit > 0, "Limit must be >0");
        checkNotNull(consistency, "consistency");

        LimitCounter remaining = new LimitCounter(limit);
        return remaining.limit(scan(tableName, fromKeyExclusive, remaining, includeDeletes, consistency));
    }

    // Internal API used by table DAOs that supports a LimitCounter instead of a long limit.
    @Override
    public Iterator<Map<String, Object>> scan(String tableName, @Nullable String fromKeyExclusive,
                                              LimitCounter limit, ReadConsistency consistency) {
        return scan(tableName, fromKeyExclusive, limit, false, consistency);
    }

    private Iterator<Map<String, Object>> scan(String tableName, @Nullable String fromKeyExclusive,
                                               LimitCounter limit, boolean includeDeletes, ReadConsistency consistency) {
        checkLegalTableName(tableName);
        checkArgument(limit.remaining() > 0, "Limit must be >0");
        checkNotNull(consistency, "consistency");

        Table table = _tableDao.get(tableName);
        Iterator<Record> records = _dataReaderDao.scan(table, fromKeyExclusive, limit, consistency);
        return resolveScanResults(records, consistency, includeDeletes);
    }

    @Override
    public Collection<String> getSplits(String tableName, int desiredRecordsPerSplit) {
        checkLegalTableName(tableName);
        checkArgument(desiredRecordsPerSplit > 0, "DesiredRecordsPerSplit must be >0");

        Table table = _tableDao.get(tableName);
        return _dataReaderDao.getSplits(table, desiredRecordsPerSplit);
    }

    @Override
    public Iterator<Map<String, Object>> getSplit(String tableName, String split,
                                                  @Nullable String fromKeyExclusive,
                                                  long limit, boolean includeDeletes, ReadConsistency consistency) {
        checkLegalTableName(tableName);
        checkNotNull(split, "split");
        checkArgument(limit > 0, "Limit must be >0");
        checkNotNull(consistency, "consistency");

        Table table = _tableDao.get(tableName);
        LimitCounter remaining = new LimitCounter(limit);
        Iterator<Record> records = _dataReaderDao.getSplit(table, split, fromKeyExclusive, remaining, consistency);
        return remaining.limit(resolveScanResults(records, consistency, includeDeletes));
    }

    @Override
    public Iterator<Map<String, Object>> multiGet(List<Coordinate> coordinates) {
        return multiGet(coordinates, ReadConsistency.STRONG);
    }

    @Override
    public Iterator<Map<String, Object>> multiGet(List<Coordinate> coordinates, ReadConsistency consistency) {
        checkNotNull(coordinates, "coordinates");
        checkNotNull(consistency, "consistency");

        AnnotatedGet multiGet = prepareGetAnnotated(consistency);
        for (Coordinate coordinate : coordinates) {
            multiGet.add(coordinate.getTable(), coordinate.getId());
        }

        return Iterators.transform(multiGet.execute(), new Function<AnnotatedContent, Map<String, Object>>() {
            @Override
            public Map<String, Object> apply(AnnotatedContent input) {
                return input.getContent();
            }
        });
    }

    private Iterator<Map<String, Object>> resolveScanResults(final Iterator<Record> records,
                                                             final ReadConsistency consistency,
                                                             final boolean includeDeletes) {
        return new AbstractIterator<Map<String, Object>>() {
            @Override
            protected Map<String, Object> computeNext() {
                while (records.hasNext()) {
                    Record record = records.next();

                    // Collapse the deltas into a Resolved object.
                    Resolved resolved = resolve(record, consistency);

                    // Skip deleted objects, if not desired
                    if (!includeDeletes && !resolved.matches(Conditions.isDefined())) {
                        continue;
                    }

                    // Convert to the final JSON format including intrinsic fields
                    return toContent(resolved, consistency);
                }
                return endOfData();
            }
        };
    }

    @Override
    public void update(String table, String key, UUID changeId, Delta delta, Audit audit) {
        update(table, key, changeId, delta, audit, WriteConsistency.STRONG);
    }

    @Override
    public void update(String table, String key, UUID changeId, Delta delta, Audit audit, WriteConsistency consistency) {
        updateAll(Collections.singletonList(new Update(table, key, changeId, delta, audit, consistency)));
    }

    @Override
    public void updateAll(Iterable<Update> updates) {
        updateAll(updates, false, ImmutableSet.<String>of());
    }

    @Override
    public void updateAll(Iterable<Update> updates, Set<String> tags) {
        updateAll(updates, false, tags);

    }

    private void updateAll(Iterable<Update> updates, final boolean isFacade,
                           @NotNull final Set<String> tags) {
        checkNotNull(updates, "updates");
        checkLegalTags(tags);
        checkNotNull(tags, "tags");
        Iterator<Update> updatesIter = updates.iterator();
        if (!updatesIter.hasNext()) {
            return;
        }

        _dataWriterDao.updateAll(Iterators.transform(updatesIter, new Function<Update, RecordUpdate>() {
            @Override
            public RecordUpdate apply(Update update) {
                checkNotNull(update, "update");
                String tableName = update.getTable();
                String key = update.getKey();
                UUID changeId = update.getChangeId();
                Delta delta = update.getDelta();
                Audit audit = update.getAudit();

                // Strip intrinsics and "~tags".  Verify the Delta results in a top-level object.
                delta = SanitizeDeltaVisitor.sanitize(delta);

                Table table = _tableDao.get(tableName);

                if (isFacade && !table.isFacade()) {
                    // Someone is trying to update a facade, but is inadvertently going to update the primary table in this dc
                    throw new SecurityException("Access denied. Update intended for a facade, but the table would be updated.");
                }

                if (table.isFacade() && !isFacade) {
                    throw new SecurityException("Access denied. Unauthorized attempt to update a facade.");
                }

                // We'll likely fail to resolve write conflicts if deltas are written into the far past after
                // compaction may have occurred.
                if (TimeUUIDs.getTimeMillis(changeId) <= _dataWriterDao.getFullConsistencyTimestamp(table)) {
                    throw new IllegalArgumentException(
                            "The 'changeId' UUID is from too far in the past: " + TimeUUIDs.getDate(changeId));
                }

                return new RecordUpdate(table, key, changeId, delta, audit, tags, update.getConsistency());
            }
        }), new DataWriterDAO.UpdateListener() {
            @Override
            public void beforeWrite(Collection<RecordUpdate> updateBatch) {
                // Tell the databus we're about to write.
                // Algorithm note: It is worth mentioning here how we make sure our data bus listeners do not lose updates.
                // 1. We always write to databus *before* writing to SoR. If we fail to write to databus, then we also fail
                //    to update SoR.
                // 2. Databus event UpdateRef has the coordinate (table/row-id) and the change-Id (time uuid) of the update
                // 3. When the event is polled, the poller makes sure that the change Id for a given event is present in the fetched record,
                //    or the change id is before the full consistency timestamp. If not, it will skip the event and get to it later.
                // Notes:
                // If the update fails to get written to SoR, its just a phantom event on the databus, and listeners will see a duplicate.
                // If the update isn't replicated to another datacenter SoR, but the databus event is, then poller will just wait for replication to finish
                // before polling the event.

                List<UpdateRef> updateRefs = Lists.newArrayListWithCapacity(updateBatch.size());
                for (RecordUpdate update : updateBatch) {
                    if (!update.getTable().isInternal()) {
                        updateRefs.add(new UpdateRef(update.getTable().getName(), update.getKey(), update.getChangeId(), tags));
                    }
                }
                if (!updateRefs.isEmpty()) {
                    _eventBus.post(new UpdateIntentEvent(this, updateRefs));
                }
            }
        });
    }

    /**
     * Facade related methods
     **/
    @Override
    public void createFacade(String table, FacadeOptions facadeOptions, Audit audit) {
        checkLegalTableName(table);
        checkNotNull(facadeOptions, "facadeDefinition");
        checkNotNull(audit, "audit");
        _tableDao.createFacade(table, facadeOptions, audit);
    }

    @Override
    public void updateAllForFacade(Iterable<Update> updates) {
        updateAll(updates, true, ImmutableSet.<String>of());
    }

    @Override
    public void updateAllForFacade(Iterable<Update> updates, Set<String> tags) {
        updateAll(updates, true, tags);
    }

    @Override
    public void dropFacade(String table, String placement, Audit audit)
            throws UnknownTableException {
        checkLegalTableName(table);
        checkNotNull(placement, "placement");
        checkNotNull(audit, "audit");
        _tableDao.dropFacade(table, placement, audit);
    }

    @Override
    public void compact(String tableName, String key, @Nullable Duration ttlOverride, ReadConsistency readConsistency, WriteConsistency writeConsistency) {
        checkLegalTableName(tableName);
        checkNotNull(key, "key");
        checkNotNull(readConsistency, "readConsistency");
        checkNotNull(writeConsistency, "writeConsistency");

        Table table = _tableDao.get(tableName);

        // Query from the database
        Record record = _dataReaderDao.read(new Key(table, key), readConsistency);

        // Resolve the timeline into a flattened object
        Expanded expanded;
        if (ttlOverride != null) {
            // The caller can override DataWriterDAO.getFullConsistencyTimestamp() for debugging. Use with caution!
            long overriddenfullConsistencyTimestamp = System.currentTimeMillis() - ttlOverride.toMillis();
            expanded = expand(record, overriddenfullConsistencyTimestamp, overriddenfullConsistencyTimestamp, Long.MIN_VALUE, true,
                    readConsistency);
        } else {
            expanded = expand(record, true, readConsistency);
        }

        // Write the results of compaction back to Cassandra
        if (expanded.getPendingCompaction() != null) {
            doCompact(table, key, expanded.getPendingCompaction(), writeConsistency);
        }
    }

    @Override
    public Collection<String> getTablePlacements() {
        return getTablePlacements(true, false);
    }

    @Override
    public Collection<String> getTablePlacements(boolean includeInternal, boolean localOnly) {
        return _tableDao.getTablePlacements(includeInternal, localOnly);
    }

    private void compactAsync(final Table table, final String key, final PendingCompaction pendingCompaction) {
        try {
            _compactionExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    // We should always write this with strong consistency
                    doCompact(table, key, pendingCompaction, WriteConsistency.STRONG);
                }
            });
        } catch (RejectedExecutionException ex) {
            _discardedCompactions.mark();
            decrementDeltaSizes(pendingCompaction);
        }
    }

    private void doCompact(Table table, String key, PendingCompaction pendingCompaction, WriteConsistency consistency) {
        // Make sure you capture deltaHistory before saving compaction to disk.
        // Otherwise, we will lose the compacted away deltas.
        List<History> deltaHistory = getDeltaHistory(table, key, pendingCompaction);

        // Save the compaction result to disk
        try {
            _dataWriterDao.compact(table, key, pendingCompaction.getCompactionKey(), pendingCompaction.getCompaction(),
                    pendingCompaction.getChangeId(), pendingCompaction.getDelta(), pendingCompaction.getKeysToDelete(), deltaHistory, consistency);
        } finally {
            // Decrement delta sizes
            decrementDeltaSizes(pendingCompaction);
        }
    }

    private List<History> getDeltaHistory(Table table, String key, PendingCompaction pendingCompaction) {
        // Check if delta history is disabled by setting the TTL to zero or deltaArchives are empty
        if (Duration.ZERO.equals(_auditStore.getHistoryTtl()) || pendingCompaction.getDeltasToArchive().isEmpty()) {
            return Lists.newArrayList();
        }
        MutableIntrinsics intrinsics = MutableIntrinsics.create(new Key(table, key));
        Iterator<DataAudit> dataAudits = _compactor.getAuditedContent(pendingCompaction, intrinsics);
        List<History> deltaHistory = Lists.newArrayList();
        while (dataAudits.hasNext()) {
            DataAudit dataAudit = dataAudits.next();
            Resolved resolved = dataAudit.getResolved();
            deltaHistory.add(new History(dataAudit.getChangeId(), toContent(resolved, ReadConsistency.STRONG),
                    dataAudit.getDelta()));
        }
        return deltaHistory;
    }

    @Override
    public ScanRangeSplits getScanRangeSplits(String placement, int desiredRecordsPerSplit, Optional<ScanRange> subrange) {
        checkNotNull(placement, "placement");
        return _dataReaderDao.getScanRangeSplits(placement, desiredRecordsPerSplit, subrange);
    }

    @Override
    public String getPlacementCluster(String placement) {
        checkNotNull(placement, "placement");
        return _dataReaderDao.getPlacementCluster(placement);
    }

    @Override
    public Iterator<MultiTableScanResult> multiTableScan(MultiTableScanOptions query, TableSet tables, LimitCounter limit, ReadConsistency consistency, @Nullable Instant cutoffTime) {
        checkNotNull(query, "query");
        checkNotNull(tables, "tables");
        checkNotNull(limit, "limit");
        checkNotNull(consistency, "consistency");
        return _dataReaderDao.multiTableScan(query, tables, limit, consistency, cutoffTime);
    }

    @Override
    public Iterator<MultiTableScanResult> stashMultiTableScan(String stashId, String placement, ScanRange scanRange, LimitCounter limit,
                                                              ReadConsistency consistency, @Nullable Instant cutoffTime) {
        checkNotNull(stashId, "stashId");
        checkNotNull(placement, "placement");
        checkNotNull(scanRange, "scanRange");
        checkNotNull(limit, "limit");
        checkNotNull(consistency, "consistency");

        // Since the range may wrap from high to low end of the token range we need to unwrap it
        List<ScanRange> unwrappedRanges = scanRange.unwrapped();

        Iterator<StashTokenRange> stashTokenRanges = Iterators.concat(
                Iterators.transform(
                        unwrappedRanges.iterator(),
                        unwrappedRange -> _stashTableDao.getStashTokenRangesFromSnapshot(stashId, placement, unwrappedRange.getFrom(), unwrappedRange.getTo())));

        return Iterators.concat(
                Iterators.transform(stashTokenRanges, stashTokenRange -> {
                    // Create a table set which always returns the table, since we know all records in this range come
                    // exclusively from this table.
                    TableSet tableSet = new TableSet() {
                        @Override
                        public Table getByUuid(long uuid)
                                throws UnknownTableException, DroppedTableException {
                            return stashTokenRange.getTable();
                        }

                        @Override
                        public void close()
                                throws IOException {
                            // No-op
                        }
                    };

                    MultiTableScanOptions tableQuery = new MultiTableScanOptions()
                            .setScanRange(ScanRange.create(stashTokenRange.getFrom(), stashTokenRange.getTo()))
                            .setPlacement(placement)
                            .setIncludeDeletedTables(false)
                            .setIncludeMirrorTables(false);

                    return multiTableScan(tableQuery, tableSet, limit, consistency, cutoffTime);
                })
        );
    }

    @Override
    public Map<String, Object> toContent(MultiTableScanResult result, ReadConsistency consistency, boolean allowAsyncCompaction) {
        return toContent(resolve(result.getRecord(), consistency, allowAsyncCompaction), consistency);
    }

    @Override
    public TableSet createTableSet() {
        return _tableDao.createTableSet();
    }

    private void checkLegalTableName(String tableName) {
        checkValidTableOrAttributeName("Table", tableName);
    }

    private void checkLegalTableAttributes(Map<String, ?> template) {
        checkNotNull(template, "template");
        for (String attributeName : template.keySet()) {
            checkArgument(Names.isLegalTableAttributeName(attributeName), "Table attribute names cannot start with '~'");
        }
    }

    private void checkLegalTags(Set<String> tags) {
        // We need to keep tags from exploding in size. So we will restrain it to only 3 tags,
        // max 8 chars each
        for (String tag : tags) {
            checkArgument(tag.length() < 9,
                    format("Tag %s is of more than the allowed length of 8 characters.", tag));
        }
        checkArgument(tags.size() <= 3, "Maximum of 3 tags are allowed");
    }

    private void checkValidTableOrAttributeName(String tableOrAttribute, String name) {
        checkArgument(Names.isLegalTableName(name),
                format("%s name must be a lowercase ASCII string between 1 and 255 characters in length. " +
                        "Allowed punctuation characters are -.:@_ and the table name may not start with a single underscore character. " +
                        "An example of a valid table name would be 'review:testcustomer'.", tableOrAttribute));

    }

    @Override
    public URI getStashRoot()
            throws StashNotAvailableException {
        if (!_stashRootDirectory.isPresent()) {
            throw new StashNotAvailableException();
        }
        return _stashRootDirectory.get();
    }

    @Override
    public void createStashTokenRangeSnapshot(String stashId, Set<String> placements) {
        checkNotNull(stashId, "stashId");
        checkNotNull(placements, "placements");
        checkState(_stashTableDao != null, "Cannot create stash snapshot without a StashTableDAO implementation");
        _stashTableDao.createStashTokenRangeSnapshot(stashId, placements, _stashBlackListTableCondition);
    }

    @Override
    public void clearStashTokenRangeSnapshot(String stashId) {
        checkNotNull(stashId, "stashId");
        checkState(_stashTableDao != null, "Cannot clear stash snapshot without a StashTableDAO implementation");
        _stashTableDao.clearStashTokenRangeSnapshot(stashId);
    }

    private void decrementDeltaSizes(PendingCompaction pendingCompaction) {
        // How many delta archives are we holding in memory for this pending compaction?
        for (Map.Entry<UUID, Delta> entry : pendingCompaction.getDeltasToArchive()) {
            _archiveDeltaSize.dec(entry.getValue().size());
        }
    }

    private String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.sor", "DefaultDataStore", name);
    }
}
