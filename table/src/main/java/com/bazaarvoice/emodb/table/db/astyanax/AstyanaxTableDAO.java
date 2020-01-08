package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.dropwizard.guice.SystemTablePlacement;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.FacadeExistsException;
import com.bazaarvoice.emodb.sor.api.FacadeOptions;
import com.bazaarvoice.emodb.sor.api.FacadeOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.UnknownFacadeException;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.api.UnpublishedDatabusEvent;
import com.bazaarvoice.emodb.sor.api.UnpublishedDatabusEventType;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.eval.ConditionEvaluator;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.table.db.DroppedTableException;
import com.bazaarvoice.emodb.table.db.MoveType;
import com.bazaarvoice.emodb.table.db.ShardsPerTable;
import com.bazaarvoice.emodb.table.db.StashTableDAO;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableBackingStore;
import com.bazaarvoice.emodb.table.db.TableChangesEnabled;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.bazaarvoice.emodb.table.db.TableFilterIntrinsics;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.bazaarvoice.emodb.table.db.generic.CachingTableDAORegistry;
import com.bazaarvoice.emodb.table.db.stash.StashTokenRange;
import com.bazaarvoice.emodb.table.db.tableset.BlockFileTableSet;
import com.bazaarvoice.emodb.table.db.tableset.TableSerializer;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.BiMap;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.bazaarvoice.emodb.table.db.astyanax.RowKeyUtils.LEGACY_SHARDS_LOG2;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.DROPPED;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.MIRROR_ACTIVATED;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.MIRROR_CONSISTENT;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.MIRROR_COPIED;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.MIRROR_CREATED;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.MIRROR_EXPIRED;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.MIRROR_EXPIRING;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.PRIMARY;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.PURGED_1;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.PURGED_2;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class AstyanaxTableDAO implements TableDAO, MaintenanceDAO, StashTableDAO, Managed {
    private static final Logger _log = LoggerFactory.getLogger(AstyanaxTableDAO.class);

    private static final Ordering<Comparable> NULLS_LAST = Ordering.natural().nullsLast();
    private static final Random RANDOM = new SecureRandom();

    /**
     * Wait at least 10 seconds between maintenance operations to allow for clock skew between servers.
     */
    static final Duration MIN_DELAY = Duration.ofSeconds(10);

    /**
     * The server updates its consistency timestamp value at most every 5 minutes.
     */
    static final Duration MIN_CONSISTENCY_DELAY = Duration.ofMinutes(5);

    /**
     * Time to wait for all readers to discover promote has occurred, time to support getSplit() calls w/old uuid.
     */
    static final Duration MOVE_DEMOTE_TO_EXPIRE = Duration.ofDays(1);

    /**
     * Delay between dropping a table and initial purge of all the data in the table, may miss late writes.
     */
    static final Duration DROP_TO_PURGE_1 = Duration.ofDays(1);

    /**
     * Maximum time to wait for full consistency.  By this time, purge will find everything.
     */
    static final Duration DROP_TO_PURGE_2 = Duration.ofDays(10);

    /**
     * Reserved key used by {@link #createTableSet()} to identify bootstrap tables.
     */
    static final String TABLE_SET_BOOTSTRAP_KEY = "~bootstrap";

    private TableBackingStore _backingStore;
    private final String _systemTablePlacement;
    private final String _systemTable;
    private final String _systemTableUuid;
    private final String _systemTableUnPublishedDatabusEvents;
    private final String _selfDataCenter;
    private final int _defaultShardsLog2;
    private final BiMap<String, Long> _bootstrapTables;
    private final Set<Long> _reservedUuids;
    private final PlacementFactory _placementFactory;
    private final PlacementCache _placementCache;
    private final RateLimiterCache _rateLimiterCache;
    private final DataCopyDAO _dataCopyDAO;
    private final DataPurgeDAO _dataPurgeDAO;
    private final FullConsistencyTimeProvider _fullConsistencyTimeProvider;
    private final ValueStore<Boolean> _tableChangesEnabled;
    private final CacheHandle _tableCacheHandle;
    private final Map<String, String> _placementsUnderMove;
    private CQLStashTableDAO _stashTableDao;
    private Clock _clock;

    @Inject
    public AstyanaxTableDAO(LifeCycleRegistry lifeCycle,
                            @SystemTableNamespace String systemTableNamespace,
                            @SystemTablePlacement String systemTablePlacement,
                            @ShardsPerTable int defaultNumShards,
                            @BootstrapTables Map<String, Long> bootstrapTables,
                            PlacementFactory placementFactory,
                            PlacementCache placementCache,
                            @CurrentDataCenter String selfDataCenter,
                            @Maintenance RateLimiterCache rateLimiterCache,
                            DataCopyDAO dataCopyDAO,
                            DataPurgeDAO dataPurgeDAO,
                            FullConsistencyTimeProvider fullConsistencyTimeProvider,
                            @TableChangesEnabled ValueStore<Boolean> tableChangesEnabled,
                            @CachingTableDAORegistry CacheRegistry cacheRegistry,
                            @PlacementsUnderMove Map<String, String> placementsUnderMove,
                            @Nullable Clock clock) {
        _systemTablePlacement = checkNotNull(systemTablePlacement, "systemTablePlacement");
        _bootstrapTables = HashBiMap.create(checkNotNull(bootstrapTables, "bootstrapTables"));
        _reservedUuids = _bootstrapTables.inverse().keySet();
        _placementFactory = checkNotNull(placementFactory);
        _placementCache = checkNotNull(placementCache, "placementCache");
        _selfDataCenter = checkNotNull(selfDataCenter, "selfDataCenter");
        _defaultShardsLog2 = RowKeyUtils.computeShardsLog2(defaultNumShards, "default");
        _rateLimiterCache = checkNotNull(rateLimiterCache, "rateLimiterCache");
        _dataCopyDAO = checkNotNull(dataCopyDAO, "copyDataDAO");
        _dataPurgeDAO = checkNotNull(dataPurgeDAO, "purgeDataDAO");
        _fullConsistencyTimeProvider = checkNotNull(fullConsistencyTimeProvider, "fullConsistencyTimeProvider");
        _tableChangesEnabled = checkNotNull(tableChangesEnabled, "tableChangesEnabled");
        _tableCacheHandle = cacheRegistry.lookup("tables", true);
        _placementsUnderMove = checkNotNull(placementsUnderMove, "placementsUnderMove");
        _clock = clock != null ? clock : Clock.systemUTC();

        // There are two tables used to store metadata about all the other tables.
        checkNotNull(systemTableNamespace, "systemTableNamespace");
        _systemTable = systemTableNamespace + ":table";
        _systemTableUuid = systemTableNamespace + ":table_uuid";
        String systemDataCenterTable = systemTableNamespace + ":data_center";
        _systemTableUnPublishedDatabusEvents = systemTableNamespace + ":table_unpublished_databus_events";

        // If this table DAO uses itself to store its table metadata (ie. it requires bootstrap tables) then make sure
        // the right bootstrap tables are specified.  This happens when "_dataStore" uses "this" for table metadata.
        if (!_bootstrapTables.isEmpty()) {
            Set<String> expectedTables = ImmutableSet.of(_systemTable, _systemTableUuid, systemDataCenterTable, _systemTableUnPublishedDatabusEvents);
            Set<String> diff = Sets.symmetricDifference(expectedTables, bootstrapTables.keySet());
            checkState(diff.isEmpty(), "Bootstrap tables map is missing tables or has extra tables: %s", diff);
        }

        lifeCycle.manage(this);
    }

    @Inject
    public void setBackingStore(TableBackingStore backingStore) {
        _backingStore = backingStore;
    }

    /**
     * Optional binding, required only if running in stash mode.
     */
    @Inject (optional = true)
    public void setCQLStashTableDAO(CQLStashTableDAO stashTableDao) {
        _stashTableDao = stashTableDao;
    }

    @Override
    public void start()
            throws Exception {
        // Ensure the basic system tables exist.  For the DataStore these will be bootstrap tables.
        for (String table : new String[] {_systemTable, _systemTableUuid, _systemTableUnPublishedDatabusEvents}) {
            TableOptions options = new TableOptionsBuilder().setPlacement(_systemTablePlacement).build();
            Audit audit = new AuditBuilder().setComment("initial startup").setLocalHost().build();
            _backingStore.createTable(table, options, ImmutableMap.<String, Object>of(), audit);
        }
    }

    @Override
    public void stop()
            throws Exception {
        // Do nothing
    }

    // MaintenanceDAO
    @Override
    public Iterator<Map.Entry<String, MaintenanceOp>> listMaintenanceOps() {
        final Iterator<Map<String, Object>> tableIter =
                _backingStore.scan(_systemTable, null, LimitCounter.max(), ReadConsistency.STRONG);
        return new AbstractIterator<Map.Entry<String, MaintenanceOp>>() {
            @Override
            protected Map.Entry<String, MaintenanceOp> computeNext() {
                while (tableIter.hasNext()) {
                    TableJson json = new TableJson(tableIter.next());
                    MaintenanceOp op = getNextMaintenanceOp(json, false/*don't expose task outside this class*/);
                    if (op != null) {
                        return Maps.immutableEntry(json.getTable(), op);
                    }
                }
                return endOfData();
            }
        };
    }

    // MaintenanceDAO
    @Nullable
    @Override
    public MaintenanceOp getNextMaintenanceOp(String table) {
        TableJson json = readTableJson(table, false);
        return getNextMaintenanceOp(json, false/*don't expose task outside this class*/);
    }

    // MaintenanceDAO
    @Override
    public void performMetadataMaintenance(String table) {
        TableJson json = readTableJson(table, false);
        MaintenanceOp op = getNextMaintenanceOp(json, true);
        if (op == null || op.getWhen().isAfter(_clock.instant()) || op.getType() != MaintenanceType.METADATA) {
            return;  // Nothing to do
        }

        // Do the maintenance.
        op.getTask().run(null);
    }

    // MaintenanceDAO
    @Override
    public void performDataMaintenance(String table, Runnable progress) {
        TableJson json = readTableJson(table, false);
        MaintenanceOp op = getNextMaintenanceOp(json, true);
        if (op == null || op.getWhen().isAfter(_clock.instant()) || op.getType() != MaintenanceType.DATA ||
                !_selfDataCenter.equals(op.getDataCenter())) {
            return;  // Nothing to do
        }

        // Do the maintenance
        op.getTask().run(progress);
    }

    /**
     * Returns the next maintenance operation that should be performed on the specified table.
     */
    @Nullable
    private MaintenanceOp getNextMaintenanceOp(final TableJson json, boolean includeTask) {
        if (json.isDeleted() || json.getStorages().isEmpty()) {
            return null;
        }

        // Loop through the table uuids and pick the MaintenanceOp that should execute first, with the minimum "when".
        MaintenanceOp op = NULLS_LAST.min(Iterables.transform(json.getStorages(), new Function<Storage, MaintenanceOp>() {
            @Override
            public MaintenanceOp apply(Storage storage) {
                return getNextMaintenanceOp(json, storage);
            }
        }));
        // Don't expose the MaintenanceOp Runnable to most callers.  It may only be run from the right data center
        // and with the right locks in place so it's best to not leak it to where it may be called accidentally.
        if (op != null && !includeTask) {
            op.clearTask();
        }
        return op;
    }

    /**
     * Helper returns the next maintenance operation that should be performed on the specified table uuid/storage.
     */
    @Nullable
    private MaintenanceOp getNextMaintenanceOp(final TableJson json, final Storage storage) {
        final StorageState from = storage.getState();
        switch (from) {
            case PRIMARY:
                return null;  // Nothing to do.  This is the common case.

            case MIRROR_CREATED: {  // Initial mirror create/activate failed, retry the operation.
                from.getTransitionedAt(storage);
                Instant when = _clock.instant();  // Retry immediately.
                return MaintenanceOp.forMetadata("Move:create-mirror", when, new MaintenanceTask() {
                    @Override
                    public void run(Runnable ignored) {
                        // Retry mirror creation.
                        moveStart(json, storage.getPrimary(), storage.getUuidString(), storage.getPlacement(),
                                storage.getShardsLog2(), "doMoveCreateMirror", Optional.<Audit>absent(),
                                (storage.isPlacementMove()) ? MoveType.FULL_PLACEMENT : MoveType.SINGLE_TABLE);

                        // No exceptions?  That means every data center knows about the new mirror.

                        stateTransition(json, storage, MIRROR_CREATED, MIRROR_ACTIVATED, InvalidationScope.GLOBAL);
                    }
                });
            }

            case MIRROR_ACTIVATED: {
                final Instant transitionedAt = storage.getTransitionedTimestamp(from);
                Instant when = transitionedAt.plus(MIN_CONSISTENCY_DELAY);
                String dataCenter = selectDataCenterForPlacements(storage.getPlacement(), storage.getPrimary().getPlacement());
                return MaintenanceOp.forData("Move:copy-data", when, dataCenter, new MaintenanceTask() {
                    @Override
                    public void run(Runnable progress) {
                        // Wait until writes in-flight at the time of mirror activation have replicated so copy doesn't miss anything.
                        checkPlacementConsistent(storage.getPrimary().getPlacement(), transitionedAt);

                        copyData(json, storage.getPrimary(), storage, progress);

                        // The next step occurs in the same data center so no need to write/flush globally.
                        stateTransition(json, storage, from, MIRROR_COPIED, InvalidationScope.DATA_CENTER);
                    }
                });
            }

            case MIRROR_COPIED: {
                final Instant transitionedAt = storage.getTransitionedTimestamp(from);
                Instant when = transitionedAt.plus(MIN_CONSISTENCY_DELAY);
                String dataCenter = selectDataCenterForPlacements(storage.getPlacement(), storage.getPrimary().getPlacement());
                return MaintenanceOp.forData("Move:data-consistent", when, dataCenter, new MaintenanceTask() {
                    @Override
                    public void run(Runnable ignored) {
                        // Throw an exception (causing retry in a few minutes) if the copied data hasn't propagated to all data centers.
                        // Note: The system data center may not have access to check full consistency of a particular placement.
                        checkPlacementConsistent(storage.getPlacement(), transitionedAt);

                        stateTransition(json, storage, from, MIRROR_CONSISTENT, InvalidationScope.GLOBAL);
                    }
                });
            }

            case MIRROR_CONSISTENT:
            case PROMOTED: {  // If PROMOTED then previous promote was attempted but failed, retry the operation.
                Instant transitionedAt = storage.getTransitionedTimestamp(MIRROR_CONSISTENT);
                Instant when = transitionedAt != null ? transitionedAt.plus(MIN_DELAY) : _clock.instant();
                return MaintenanceOp.forMetadata("Move:promote-mirror", when, new MaintenanceTask() {
                    @Override
                    public void run(Runnable ignored) {
                        movePromote(json, storage);  // Moves into the PROMOTED state when it succeeds.

                        // No exceptions?  That means every data center knows about the new primary.

                        // The next step occurs in the same data center so no need to write/flush globally.
                        stateTransition(json, storage, from, PRIMARY, InvalidationScope.DATA_CENTER);
                    }
                });
            }

            case MIRROR_DEMOTED: {
                Instant transitionedAt = storage.getTransitionedTimestamp(from);
                Instant when = transitionedAt != null ? transitionedAt.plus(MIN_DELAY) : _clock.instant();
                return MaintenanceOp.forMetadata("Move:set-expiration", when, new MaintenanceTask() {
                    @Override
                    public void run(Runnable ignored) {
                        // Note: This is separate from the previous step (promote mirror) because it operates on the old primary that
                        // has been demoted to a mirror, and by separating the steps we round trip through Cassandra and verify that
                        // the promotion worked as expected.

                        Instant expiresAt = _clock.instant().plus(MOVE_DEMOTE_TO_EXPIRE);

                        // The next step occurs in the same data center so no need to write/flush globally.
                        stateTransition(json, storage, from, MIRROR_EXPIRING, expiresAt, InvalidationScope.DATA_CENTER);
                    }
                });
            }

            case MIRROR_EXPIRING:
            case MIRROR_EXPIRED: {  // If MIRROR_EXPIRED then previous expire was attempted but failed, retry the operation.
                Instant mirrorExpiresAt = storage.getMirrorExpiresAt();
                Instant when = mirrorExpiresAt != null ? mirrorExpiresAt : _clock.instant();
                return MaintenanceOp.forMetadata("Move:expire-mirror", when, new MaintenanceTask() {
                    @Override
                    public void run(Runnable ignored) {
                        // Perform a global write and cache invalidation to disable reads everywhere (getSplits).
                        stateTransition(json, storage, from, MIRROR_EXPIRED, InvalidationScope.GLOBAL);

                        // No exceptions?  That means every data center knows the mirror has expired (reads are disabled).

                        // Mark the mirror as dropped to disable writes everywhere.
                        stateTransition(json, storage, MIRROR_EXPIRED, DROPPED, InvalidationScope.GLOBAL);
                    }
                });
            }

            case DROPPED:
            case PURGED_1: {
                // Skip PURGE_1 and go straight to PURGE_2 if enough time has elapsed,
                // or if we are moving the entire placement, skip the PURGED_1 step to move as quickly as possible
                // and not
                final Instant droppedAt = storage.getTransitionedTimestamp(DROPPED);
                Instant when1 = droppedAt.plus(DROP_TO_PURGE_1);
                Instant when2 = droppedAt.plus(DROP_TO_PURGE_2);
                final int iteration = !storage.isPlacementMove() && (from == DROPPED && when2.isAfter(_clock.instant())) ? 1 : 2;
                String dataCenter = selectDataCenterForPlacements(storage.getPlacement());
                return MaintenanceOp.forData("Drop:purge-" + iteration, iteration == 1 ? when1 : when2, dataCenter, new MaintenanceTask() {
                    @Override
                    public void run(Runnable progress) {

                        // Delay the final purge until we're confident we'll catch everything.
                        if (iteration == 2) {
                            checkPlacementConsistent(storage.getPlacement(), droppedAt);
                        }

                        purgeData(json, storage, iteration, progress);

                        if (iteration == 1) {
                            stateTransition(json, storage, from, PURGED_1, InvalidationScope.DATA_CENTER);
                        } else {
                            stateTransition(json, storage, from, PURGED_2, InvalidationScope.GLOBAL);
                        }
                    }
                });
            }

            case PURGED_2: {
                Instant transitionedAt = storage.getTransitionedTimestamp(from);
                Instant when = transitionedAt.plus(MIN_CONSISTENCY_DELAY);
                return MaintenanceOp.forMetadata("Drop:delete", when, new MaintenanceTask() {
                    @Override
                    public void run(Runnable progress) {
                        deleteFinal(json, storage);  // End of the line.
                    }
                });
            }

            default:
                throw new UnsupportedOperationException(String.valueOf(from));
        }
    }

    private void stateTransition(TableJson json, Storage storage, StorageState from, StorageState to,
                                 InvalidationScope scope) {
        stateTransition(json, storage.getUuidString(), storage.getPlacement(), from, to, _clock.instant(), scope);
    }

    private void stateTransition(TableJson json, Storage storage, StorageState from, StorageState to,
                                 Instant markerValue, InvalidationScope scope) {
        stateTransition(json, storage.getUuidString(), storage.getPlacement(), from, to, markerValue, scope);
    }

    private void stateTransition(TableJson json, String storageUuid, String storagePlacement,
                                 StorageState from, StorageState to, Instant markerValue,
                                 InvalidationScope scope) {
        _log.info("State transition for table '{}' and storage '{}' from={} to={}.",
                json.getTable(), storageUuid, from, to);

        Delta delta = json.newNextState(storageUuid, to, markerValue);
        Audit audit = new AuditBuilder()
                .set("_op", describeTransition(from, to))
                .set("_uuid", storageUuid)
                .set("_placement", storagePlacement)
                .build();
        updateTableMetadata(json.getTable(), delta, audit, scope);
    }

    private String describeTransition(StorageState from, StorageState to) {
        return String.format("do%sTo%s",
                CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, from.name()),
                CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, to.name()));
    }

    /**
     * Write the delta to the system table and invalidate caches in the specified scope.
     */
    private void updateTableMetadata(String table, Delta delta, Audit audit, @Nullable InvalidationScope scope) {
        _backingStore.update(_systemTable, table, TimeUUIDs.newUUID(), delta, audit,
                scope == InvalidationScope.GLOBAL ? WriteConsistency.GLOBAL : WriteConsistency.STRONG);

        // Synchronously notify other emodb servers of the table change.
        if (scope != null) {
            _tableCacheHandle.invalidate(scope, table);
        }
    }

    /* Write the delta to the unpublished databus events system table which stores all the drop, purge, attribute changes, placement moves etc information of the tables.
     */
    private void writEventToSystemTable(String key, Delta delta, Audit audit) {
        _backingStore.update(_systemTableUnPublishedDatabusEvents, key, TimeUUIDs.newUUID(), delta, audit, WriteConsistency.GLOBAL);
    }

    @Timed (name = "bv.emodb.table.AstyanaxTableDAO.create", absolute = true)
    @Override
    public void create(String name, TableOptions options, Map<String, ?> attributes, Audit audit)
            throws TableExistsException {
        checkNotNull(name, "table");
        checkNotNull(options, "options");
        checkNotNull(attributes, "attributes");
        checkNotNull(audit, "audit");

        if (_bootstrapTables.containsKey(name)) {
            throw new TableExistsException(format("May not modify system tables: %s", name), name);
        }
        checkTableChangesAllowed(name);

        // If placement move is in progress, create the new table in its new placement
        options = replacePlacementIfMoveInProgress(options);

        // Check that the table doesn't yet exist.
        Table existingTable = getInternal(name);
        if (existingTable != null) {
            if (existingTable.getOptions().getPlacement().equals(options.getPlacement())
                    && existingTable.getAttributes().equals(attributes)) {
                return;  // Nothing to do
            } else {
                throw new TableExistsException(format("Cannot create table that already exists: %s", name), name);
            }
        }

        // Check that the placement string is valid
        String placement = checkPlacement(options.getPlacement());

        // Pick a unique table uuid that will determine where in Cassandra all the data will be stored.
        String uuid = newTableUuidString(name, audit);

        // Write the new table definition to Cassandra.
        Delta delta = TableJson.newCreateTable(uuid, attributes, placement, _defaultShardsLog2);

        Audit augmentedAudit = AuditBuilder.from(audit)
                .set("_op", "create")
                .set("_uuid", uuid)
                .build();
        updateTableMetadata(name, delta, augmentedAudit, InvalidationScope.GLOBAL);
    }

    @Timed (name = "bv.emodb.table.AstyanaxTableDAO.createFacade", absolute = true)
    @Override
    public void createFacade(String name, FacadeOptions facadeOptions, Audit audit)
            throws FacadeExistsException {
        checkNotNull(name, "table");
        checkNotNull(facadeOptions, "facadeDefinition");
        checkNotNull(audit, "audit");

        checkTableChangesAllowed(name);

        // If placement move is in progress, create the new facade in its new placement
        facadeOptions = replacePlacementIfMoveInProgress(facadeOptions);

        if (!checkFacadeAllowed(name, facadeOptions)) {
            return;  // Nothing to do
        }

        // Check that the placement string is valid
        String placement = checkPlacement(facadeOptions.getPlacement());

        // Pick a unique table uuid that will determine where in Cassandra all the data will be stored.
        String uuid = newTableUuidString(name, audit);

        // Write the new facade definition to Cassandra.
        Delta delta = TableJson.newCreateFacade(uuid, placement, _defaultShardsLog2);

        Audit augmentedAudit = AuditBuilder.from(audit)
                .set("_op", "createFacade")
                .set("_uuid", uuid)
                .set("_placement", facadeOptions.getPlacement())
                .build();
        updateTableMetadata(name, delta, augmentedAudit, InvalidationScope.GLOBAL);
    }

    /**
     * Returns true if facade may be created for the specified table and placement.
     * Throws an exception if a facade is not allowed because of a conflict with the master or another facade.
     * Returns false if there is already a facade at the specified placement, so facade creation would be idempotent.
     */
    @Override
    public boolean checkFacadeAllowed(String table, FacadeOptions options)
            throws FacadeExistsException {
        return checkFacadeAllowed(readTableJson(table, true), options.getPlacement(), null);
    }

    private boolean checkFacadeAllowed(TableJson json, String newPlacement, @Nullable String ignoreFacadeUuid)
            throws TableExistsException {
        // Check if new facade placement != master table placement.
        for (Storage master : json.getMasterStorage().getPrimaryAndMirrors()) {
            if (newPlacement.equals(master.getPlacement())) {
                throw new IllegalArgumentException(
                        format("Cannot create a facade in the same placement as its table: %s", newPlacement));
            }
        }

        // Make sure that facades don't overlap.  In every data center it must be unambiguous which facade applies.
        for (Storage facade : json.getFacades()) {
            if (facade.getUuidString().equals(ignoreFacadeUuid)) {
                continue;
            }
            if (newPlacement.equals(facade.getPlacement())) {
                return false;  // Idempotent.
            }
            for (Storage storage : facade.getPrimaryAndMirrors()) {
                if (selectDataCenterForPlacements(newPlacement, storage.getPlacement()) != null) {
                    throw new FacadeExistsException(
                            format("Cannot create a facade in this placement as it will overlap with other facade placements: %s (on %s)",
                                    json.getTable(), storage.getPlacement()),
                            json.getTable(), facade.getPlacement());
                }
            }
        }
        return true;
    }

    @Timed (name = "bv.emodb.table.AstyanaxTableDAO.drop", absolute = true)
    @Override
    public void drop(String name, Audit audit)
            throws UnknownTableException {
        checkNotNull(name, "table");
        checkNotNull(audit, "audit");

        checkTableChangesAllowed(name);

        // Dropping a table progresses through the following steps:
        // 1. [SYSTEM DATA CENTER] Mark the uuid as dropped.
        // 2. [LOCAL DATA CENTER] Purge the data from Cassandra (initial cleanup).
        // -- Wait a while to ensure all servers in the cluster have stopped writing to src and all eventually
        //    consistent writes have been applied.
        // 3. [LOCAL DATA CENTER] Purge the data from Cassandra (final cleanup).
        // 4. [SYSTEM DATA CENTER] Delete the uuid from the storage map.  If the table hasn't been re-created,
        //    delete the table json completely from the backing store.

        // Read the table metadata from the DataStore (this is often a recursive call into the DataStore).  Do this
        // directly instead of using get()/getInternal() to avoid validation checks so we can drop an invalid table.
        TableJson json = readTableJson(name, true);

        // write about the drop operation (metadata changed info) to a special system table.
        writeUnpublishedDatabusEvent(name, UnpublishedDatabusEventType.DROP_TABLE);

        // now, update the Table Metadata
        Delta delta = json.newDropTable();

        Audit augmentedAudit = AuditBuilder.from(audit)
                .set("_op", "drop")
                .set("_uuid", json.getMasterStorage().getUuidString())
                .build();
        updateTableMetadata(json.getTable(), delta, augmentedAudit, InvalidationScope.GLOBAL);
    }

    @Override
    public void dropFacade(String name, String placement, Audit audit)
            throws UnknownFacadeException {
        checkNotNull(name, "table");
        checkNotNull(audit, "audit");
        checkPlacement(placement);

        checkTableChangesAllowed(name);

        // Read the table metadata from the DataStore (this is often a recursive call into the DataStore).  Do this
        // directly instead of using get()/getInternal() to avoid validation checks so we can drop an invalid table.
        TableJson json = readTableJson(name, true);

        // write about the drop operation (metadata changed info) to a special system table.
        writeUnpublishedDatabusEvent(name, UnpublishedDatabusEventType.DROP_FACADE);

        // Find the facade for the specified placement.
        Storage facadeStorage = json.getFacadeForPlacement(placement);

        Delta delta = json.newDropFacade(facadeStorage);

        Audit augmentedAudit = AuditBuilder.from(audit)
                .set("_op", "dropFacade")
                .set("_uuid", json.getMasterStorage().getUuidString())
                .build();
        updateTableMetadata(json.getTable(), delta, augmentedAudit, InvalidationScope.GLOBAL);
    }

    /**
     * Purge a dropped table or facade.  Executed twice, once for fast cleanup and again much later for stragglers.
     */
    private void purgeData(TableJson json, Storage storage, int iteration, Runnable progress) {
        _log.info("Purging data for table '{}' and table uuid '{}' (facade={}, iteration={}).",
                json.getTable(), storage.getUuidString(), storage.isFacade(), iteration);

        // Cap the # of writes-per-second to avoid saturating the cluster.
        Runnable rateLimitedProgress = rateLimited(_placementCache.get(storage.getPlacement()), progress);

        // Delete all the data for this table.
        audit(json.getTable(), "doPurgeData" + iteration, new AuditBuilder()
                .set("_uuid", storage.getUuidString())
                .set("_placement", storage.getPlacement())
                .build());
        _dataPurgeDAO.purge(newAstyanaxStorage(storage, json.getTable()), rateLimitedProgress);
    }

    /**
     * Last step in dropping a table or facade.
     */
    private void deleteFinal(TableJson json, Storage storage) {
        // Remove the uuid and storage--we no longer need it.  Leave the uuid in _systemTableUuid so it isn't reused.
        Delta delta = json.newDeleteStorage(storage);

        Audit audit = new AuditBuilder()
                .set("_op", "doDeleteFinal")
                .set("_uuid", storage.getUuidString())
                .build();
        updateTableMetadata(json.getTable(), delta, audit, InvalidationScope.LOCAL);
    }

    @Override
    public void move(String table, String destPlacement, Optional<Integer> numShards, Audit audit, MoveType moveType)
            throws UnknownTableException {
        checkNotNull(table, "table");
        checkPlacement(destPlacement);
        checkNotNull(audit, "audit");

        checkTableChangesAllowed(table);

        // Read the existing metadata for the table.
        TableJson json = readTableJson(table, true);
        Storage srcStorage = json.getMasterStorage();

        moveInternal(table, json, srcStorage, destPlacement, numShards, audit, "move", moveType);
    }

    @Override
    public void moveFacade(String table, String sourcePlacement, String destPlacement, Optional<Integer> numShards, Audit audit, MoveType moveType)
            throws UnknownTableException {
        checkNotNull(table, "table");
        checkPlacement(sourcePlacement);
        checkPlacement(destPlacement);
        checkNotNull(audit, "audit");

        checkTableChangesAllowed(table);

        // Read the existing metadata for the table/facade.
        TableJson json = readTableJson(table, true);
        Storage srcStorage = json.getFacadeForPlacement(sourcePlacement);
        String srcUuid = srcStorage.getUuidString();

        // Don't let facades move to the same placement as the main table or overlap other facades.
        if (!checkFacadeAllowed(json, destPlacement, srcUuid)) {
            throw new FacadeExistsException(
                    format("Cannot move a facade to a placement for which another facade already exists: %s", destPlacement),
                    table, destPlacement);
        }

        moveInternal(table, json, srcStorage, destPlacement, numShards, audit, "moveFacade", moveType);
    }

    private TableOptions replacePlacementIfMoveInProgress(TableOptions options) {
        String placement = options.getPlacement();
        if (_placementsUnderMove.containsKey(placement)) {
            return new TableOptionsBuilder().setFacades(options.getFacades())
                    .setPlacement(_placementsUnderMove.get(placement)).build();
        }
        return options;
    }

    private FacadeOptions replacePlacementIfMoveInProgress(FacadeOptions options) {
        String placement = options.getPlacement();
        if (_placementsUnderMove.containsKey(placement)) {
            return new FacadeOptionsBuilder().setPlacement(_placementsUnderMove.get(placement)).build();
        }
        return options;
    }

    private void checkTableChangesAllowed(String table) {
        if (_bootstrapTables.containsKey(table)) {
            throw new IllegalArgumentException(format("May not modify system tables: %s", table));
        }
        if (!_tableChangesEnabled.get()) {
            // It's tempting to throw 503 (service unavailable) but then the client will retry and, if too many retries
            // fail, Ostrich will mark the endpoint as bad.  Return a 4xx response so the client won't retry.
            throw new WebApplicationException(Response.status(Response.Status.FORBIDDEN)
                    .entity(format("Table metadata changes have been disabled by an administrator: %s", table)).build());
        }
    }

    private void moveInternal(String table, TableJson json, Storage srcStorage, String destPlacement,
                              Optional<Integer> numShards, Audit audit, String op, MoveType moveType) {
        // Moving a table from 'src' to 'dest' progresses through several steps and phases:

        // *** PHASE 1 - COPY ***
        // 1. [SYSTEM DATA CENTER] Allocate a uuid for dest.  Configure write mirroring for src to dest.
        // -- Wait a while to ensure all servers in the cluster have begun mirroring writes.
        // 2. [LOCAL DATA CENTER] Copy the data from src to dest.

        // *** PHASE 2 - MIRROR ***
        // 3. [SYSTEM DATA CENTER] Switch readers to dest.  Configure write mirroring for dest to src.
        // -- Wait a while to ensure all servers in the cluster have begun reading from dest.
        // -- Also, wait a fixed period of time for all calls to getSplit() using the old table uuid to execute.

        // *** PHASE 3 - PURGE ***
        // 4. [SYSTEM DATA CENTER] Drop src.  Disable write mirroring.  The logic is basically the same as drop.
        // -- Wait a while to ensure all servers in the cluster have stopped writing to src and all eventually
        //    consistent writes have been applied.
        // 5. [LOCAL DATA CENTER] Purge src.
        // 6. [SYSTEM DATA CENTER] Delete src from the storage map.

        // Operations that manipulate the table metadata json in a non-trivial way are confined to the system data
        // center so they can grab system-wide locks and ensure there are no race conditions with drop table, etc.

        int shardsLog2 = numShards.isPresent() ? RowKeyUtils.computeShardsLog2(numShards.get(), "<move>") : _defaultShardsLog2;

        Storage existingMove = srcStorage.getMoveTo();

        // Check whether the current location matches the desired attributes.
        if (matches(srcStorage, destPlacement, shardsLog2)) {
            if (existingMove != null) {
                moveCancel(json, srcStorage, existingMove);
            }
            return;  // Idempotent.
        }
        // Is the requested move is already in progress or there's an existing mirror with the desired attributes?
        for (Storage mirrorTo : srcStorage.getMirrors()) {
            if (matches(mirrorTo, destPlacement, shardsLog2) && !mirrorTo.isMirrorExpired()) {
                if (mirrorTo != existingMove) {
                    // Flip back to the old location--the data should still all be there.
                    moveRestart(json, srcStorage, mirrorTo);
                }
                return;
            }
        }

        // Some data center must have access to the source and destination placements (and any existing mirrors)
        // in order to execute the copy step and perform write mirroring.  Note that once the move starts writes
        // will only succeed in data centers that have access to all the placements.
        Set<String> placements = Sets.newHashSet();
        placements.add(destPlacement);
        for (Storage mirrorTo : srcStorage.getPrimaryAndMirrors()) {
            placements.add(mirrorTo.getPlacement());
        }
        if (selectDataCenterForPlacements(placements.toArray(new String[placements.size()])) == null) {
            throw new IllegalArgumentException(format(
                    "Source and destination and mirror placements must overlap in some data center: %s",
                    Joiner.on(", ").join(Ordering.natural().immutableSortedCopy(placements))));
        }

        // Pick a unique table uuid that will be the target of the move.
        String destUuid = newTableUuidString(table, audit);

        // Update the table metadata for step 1.
        moveStart(json, srcStorage, destUuid, destPlacement, shardsLog2, op, Optional.of(audit), moveType);

        // No exceptions?  That means every data center knows about the new mirror.

        stateTransition(json, destUuid, destPlacement, MIRROR_CREATED, MIRROR_ACTIVATED, _clock.instant(), InvalidationScope.GLOBAL);
    }

    private boolean matches(Storage storage, String placement, int shardsLog2) {
        return storage.getPlacement().equals(placement) && storage.getShardsLog2() == shardsLog2;
    }

    private void moveStart(TableJson json, Storage src, String destUuid, String destPlacement, int shardsLog2,
                           String op, Optional<Audit> audit, MoveType moveType) {
        Delta delta;
        if (moveType == MoveType.FULL_PLACEMENT) {
            delta = json.newMovePlacementStart(src, destUuid, destPlacement, shardsLog2);
        } else {
            delta = json.newMoveStart(src, destUuid, destPlacement, shardsLog2);
        }
        Audit augmentedAudit = (audit.isPresent() ? AuditBuilder.from(audit.get()) : new AuditBuilder())
                .set("_op", op)
                .set("_srcUuid", src.getUuidString())
                .set("_srcPlacement", src.getPlacement())
                .set("_destUuid", destUuid)
                .set("_destPlacement", destPlacement)
                .build();
        updateTableMetadata(json.getTable(), delta, augmentedAudit, InvalidationScope.GLOBAL);
    }

    /**
     * Cancel a move before mirror promotion has taken place.
     */
    private void moveCancel(TableJson json, Storage src, Storage dest) {
        Delta delta = json.newMoveCancel(src);
        Audit audit = new AuditBuilder()
                .set("_op", "doMoveCancel")
                .set("_srcUuid", src.getUuidString())
                .set("_srcPlacement", src.getPlacement())
                .set("_destUuid", dest.getUuidString())
                .set("_destPlacement", dest.getPlacement())
                .build();
        updateTableMetadata(json.getTable(), delta, audit, InvalidationScope.GLOBAL);
    }

    /**
     * Restart a move with an existing mirror that may or may not have once been primary.
     */
    private void moveRestart(TableJson json, Storage src, Storage dest) {
        Delta delta = json.newMoveRestart(src, dest);
        Audit audit = new AuditBuilder()
                .set("_op", "doMoveRestart")
                .set("_srcUuid", src.getUuidString())
                .set("_srcPlacement", src.getPlacement())
                .set("_destUuid", dest.getUuidString())
                .set("_destPlacement", dest.getPlacement())
                .build();
        updateTableMetadata(json.getTable(), delta, audit, InvalidationScope.GLOBAL);
    }

    private void copyData(TableJson json, Storage src, Storage dest, Runnable progress) {
        // Cap the # of writes-per-second to avoid saturating the cluster.
        Runnable rateLimitedProgress = rateLimited(_placementCache.get(dest.getPlacement()), progress);

        _log.info("Moving data for table '{}' and table uuid '{}' (facade={}) from {} to {}.",
                json.getTable(), src.getUuidString(), src.isFacade(), src.getPlacement(), dest.getPlacement());
        audit(json.getTable(), "doCopyData", new AuditBuilder()
                .set("_uuid", src.getUuidString())
                .set("_placement", src.getPlacement())
                .set("_destUuid", dest.getUuidString())
                .set("_destPlacement", dest.getPlacement())
                .build());
        _dataCopyDAO.copy(newAstyanaxStorage(src, json.getTable()), newAstyanaxStorage(dest, json.getTable()), rateLimitedProgress);
    }

    private void movePromote(TableJson json, Storage mirror) {
        // write about the move operation (metadata changed info) to another system table.
        writeUnpublishedDatabusEvent(json.getTable(), UnpublishedDatabusEventType.MOVE_PLACEMENT);

        // Mark the mirror as promoted so servers will pick it as the new primary.
        Delta delta = json.newMovePromoteMirror(mirror);
        Audit audit = new AuditBuilder()
                .set("_op", "doPromoteMirror")
                .set("_uuid", mirror.getUuidString())
                .set("_placement", mirror.getPlacement())
                .build();
        updateTableMetadata(json.getTable(), delta, audit, InvalidationScope.GLOBAL);
    }

    @Override
    public void setAttributes(String name, Map<String, ?> attributes, Audit audit)
            throws UnknownTableException {
        checkNotNull(name, "table");
        checkNotNull(attributes, "attributes");

        checkTableChangesAllowed(name);

        // Throw an exception if the table doesn't exist
        TableJson json = readTableJson(name, true);

        // write about the update attributes operation (metadata changed info) to a special system table.
        writeUnpublishedDatabusEvent(name, UnpublishedDatabusEventType.UPDATE_ATTRIBUTES);

        // Write the new table attributes to Cassandra
        Delta delta = json.newSetAttributes(attributes);

        Audit augmentedAudit = AuditBuilder.from(audit)
                .set("_op", "setAttributes")
                .build();
        updateTableMetadata(json.getTable(), delta, augmentedAudit, InvalidationScope.GLOBAL);
    }

    @Timed (name = "bv.emodb.table.AstyanaxTableDAO.audit", absolute = true)
    @Override
    public void audit(String name, String op, Audit audit) {
        checkNotNull(name, "table");
        checkNotNull(audit, "audit");

        checkTableChangesAllowed(name);

        Audit augmentedAudit = AuditBuilder.from(audit)
                .set("_op", op)
                .build();
        updateTableMetadata(name, Deltas.noop(), augmentedAudit, null);
    }

    @Timed (name = "bv.emodb.table.AstyanaxTableDAO.list", absolute = true)
    @Override
    public Iterator<Table> list(@Nullable String fromNameExclusive, LimitCounter limit) {
        checkArgument(limit.remaining() > 0, "Limit must be >0");

        final Iterator<Map<String, Object>> tableIter =
                _backingStore.scan(_systemTable, fromNameExclusive, limit, ReadConsistency.STRONG);

        // Filter out dropped tables.
        return new AbstractIterator<Table>() {
            @Override
            protected Table computeNext() {
                while (tableIter.hasNext()) {
                    Table table = tableFromJson(new TableJson(tableIter.next()));
                    if (table != null) {
                        return table;
                    }
                }
                return endOfData();
            }
        };
    }

    @Override
    public boolean exists(String name) {
        return getInternal(name) != null;
    }

    @Override
    public boolean isMoveToThisPlacementAllowed(String placement) {
        return !_placementsUnderMove.containsKey(placement);
    }

    @Override
    public Table get(String name)
            throws UnknownTableException {
        Table table = getInternal(name);
        if (table == null) {
            throw new UnknownTableException(format("Unknown table: %s", name), name);
        }
        return table;
    }

    @Override
    public Table getByUuid(long uuid)
            throws UnknownTableException, DroppedTableException {
        String bootstrapTable = _bootstrapTables.inverse().get(uuid);
        if (bootstrapTable != null) {
            return loadBootstrapTable(bootstrapTable);
        }

        // Lookup the name of the table associated with this uuid.
        String tableName = getTableNameByUuid(uuid);

        // We found the table name, now lookup the rest of the table metadata.
        Table table = getInternal(tableName);
        if (table == null) {
            // The table name is only unknown if it has been deleted or possibly at some point recreated
            // and deleted again.  Convert this to the more accurate exception.
            throw new DroppedTableException(tableName);
        }
        if (!((AstyanaxTable) table).hasUUID(uuid)) {
            // The table with this name was deleted and recreated, leaving the table with our UUID orphaned.
            throw new DroppedTableException(tableName);
        }
        return table;
    }

    private String getTableNameByUuid(long uuid)
            throws UnknownTableException {
        // Lookup the name of the table associated with this uuid.
        String key = TableUuidFormat.encode(uuid);
        Map<String, Object> json = _backingStore.get(_systemTableUuid, key, ReadConsistency.STRONG);

        String tableName;
        if (json == null || (tableName = (String) json.get("table")) == null) {
            throw new UnknownTableException();
        }

        return tableName;
    }

    @Override
    public TableSet createTableSet() {
        return new BlockFileTableSet(new TableSerializer() {
            @Override
            public Set<Long> loadAndSerialize(long uuid, OutputStream out)
                    throws IOException, UnknownTableException, DroppedTableException {
                Set<Long> uuids = Sets.newHashSet();
                String jsonString;

                // Bootstrap tables aren't serialized to the output stream, just marked as bootstrap
                String bootstrapTable = _bootstrapTables.inverse().get(uuid);
                if (bootstrapTable != null) {
                    jsonString = JsonHelper.asJson(ImmutableMap.of(TABLE_SET_BOOTSTRAP_KEY, bootstrapTable));
                    uuids.add(uuid);
                } else {
                    // Convert the UUID into a table name
                    String name = getTableNameByUuid(uuid);
                    // Get the intermediate object map which defines the table
                    TableJson tableJson = readTableJson(name, false);

                    // Get all UUIDs associated with the table
                    for (Storage storage : tableJson.getStorages()) {
                        uuids.add(storage.getUuid());
                    }

                    // If the table loaded by name doesn't include this UUID then it has been dropped
                    if (tableJson.isDropped() || !uuids.contains(uuid)) {
                        throw new DroppedTableException(name);
                    }

                    // Convert the map to JSON; the string's bytes will be the table's snapshot form
                    jsonString = JsonHelper.asJson(tableJson.getRawJson());
                }

                Writer writer = new OutputStreamWriter(out, Charsets.UTF_8);
                writer.write(jsonString);
                writer.flush();

                return uuids;
            }

            @Override
            public Table deserialize(InputStream in)
                    throws IOException {
                // Convert the snapshot bytes back into a JSON string
                String jsonString = CharStreams.toString(new InputStreamReader(in, Charsets.UTF_8));
                // Convert the string back to a map
                //noinspection unchecked
                Map<String, Object> rawJson = JsonHelper.fromJson(jsonString, Map.class);
                if (rawJson.containsKey(TABLE_SET_BOOTSTRAP_KEY)) {
                    // Return the bootstrap table
                    String bootstrapTable = (String) rawJson.get(TABLE_SET_BOOTSTRAP_KEY);
                    return loadBootstrapTable(bootstrapTable);
                }
                // Convert the json back into a TableJson
                TableJson tableJson = new TableJson(rawJson);
                // Use the class method to build a table from the TableJson
                return tableFromJson(tableJson);
            }
        });
    }

    @Override
    public Collection<String> getTablePlacements(boolean includeInternal, boolean localOnly) {
        Collection<String> placements;
        if (!localOnly) {
            placements = _placementFactory.getValidPlacements();
        } else {
            placements = _placementCache.getLocalPlacements();
        }

        // Don't expose internal placement strings.
        Predicate<String> predicate = Predicates.alwaysTrue();
        if (!includeInternal) {
            predicate = new Predicate<String>() {
                @Override
                public boolean apply(String placement) {
                    return !isInternalPlacement(placement);
                }
            };
        }

        return Collections2.filter(placements, predicate);
    }

    @Nullable
    private Table getInternal(String name) {
        // Is this a bootstrap table?
        Table table = loadBootstrapTable(name);
        if (table != null) {
            return table;
        }

        // Read the table metadata from the DataStore (this is often a recursive call into the DataStore)
        return tableFromJson(readTableJson(name, false));
    }

    @VisibleForTesting
    TableJson readTableJson(String table, boolean required) {
        TableJson json = new TableJson(_backingStore.get(_systemTable, table, ReadConsistency.STRONG));
        if (required && json.isDropped()) {
            throw new UnknownTableException(format("Unknown table: %s", table), table);
        }
        return json;
    }

    /**
     * Parse the persistent JSON object into an AstyanaxTable.
     * If the master placement doesn't belong to this datacenter, this method will:
     * a. try to find a facade for this table that belongs to this datacenter
     * b. If no facade is found, it will return the table in the master placement.
     */
    @VisibleForTesting
    Table tableFromJson(TableJson json) {
        if (json.isDropped()) {
            return null;
        }
        String name = json.getTable();
        Map<String, Object> attributes = json.getAttributeMap();
        Storage masterStorage = json.getMasterStorage();
        String masterPlacement = masterStorage.getPlacement();
        Collection<Storage> facades = json.getFacades();

        TableOptions options = new TableOptionsBuilder()
                .setPlacement(masterPlacement)
                .setFacades(ImmutableList.copyOf(Iterables.transform(facades, new Function<Storage, FacadeOptions>() {
                    @Override
                    public FacadeOptions apply(Storage facade) {
                        return new FacadeOptions(facade.getPlacement());
                    }
                })))
                .build();

        Storage storageForDc = masterStorage;
        boolean available = true;
        if (!_placementFactory.isAvailablePlacement(masterPlacement)) {
            available = false;
            // The master placement does not belong to this datacenter.  Let's see if we have a
            // facade that belongs to this datacenter.  If not, we'll stick with the masterStorage.
            for (Storage facade : facades) {
                if (_placementFactory.isAvailablePlacement(facade.getPlacement())) {
                    if (storageForDc.isFacade()) {
                        // We found multiple facades for the same datacenter.
                        throw new TableExistsException(format("Multiple facades found for table %s in %s", name, _selfDataCenter), name);
                    }
                    storageForDc = facade;
                    available = true;
                }
            }
        }

        return newTable(name, options, attributes, available, storageForDc);
    }

    @Nullable
    private Table loadBootstrapTable(String name) {
        // Each system table uses a hard-coded randomly generated uuid.
        Long uuid = _bootstrapTables.get(name);
        if (uuid == null) {
            return null;  // Unknown table
        }

        TableOptions options = new TableOptionsBuilder().setPlacement(_systemTablePlacement).build();
        Map<String, Object> attributes = ImmutableMap.of();
        TableAvailability availability = new TableAvailability(_systemTablePlacement, false);
        AstyanaxStorage storage = newAstyanaxStorage(uuid, LEGACY_SHARDS_LOG2, true, _systemTablePlacement, name);
        Supplier<Collection<DataCenter>> dcSupplier = getDataCentersSupplier(_systemTablePlacement, null);
        return new AstyanaxTable(name, options, attributes, availability, storage, ImmutableList.of(storage), dcSupplier);
    }

    private Table newTable(String name, TableOptions options, Map<String, Object> attributes, boolean available, Storage storage) {
        TableAvailability availability = available ? new TableAvailability(storage.getPlacement(), storage.isFacade()) : null;

        // Reads are performed using placement and table uuid of the specified storage.
        AstyanaxStorage read = newAstyanaxStorage(storage, name);

        // If there's a move in progress, writes go to both the regular location and the move destination.
        List<AstyanaxStorage> write = Lists.newArrayList();
        write.add(read);  // Usual case.
        for (Storage mirrorTo : storage.getMirrors()) {
            write.add(newAstyanaxStorage(mirrorTo, name));  // When a move is in progress.
        }

        // Data centers for facade = all dc in the placement - dc of the master placement.
        String excludePlacement = storage.isFacade() ? options.getPlacement() : null;
        Supplier<Collection<DataCenter>> dcSupplier = getDataCentersSupplier(storage.getPlacement(), excludePlacement);

        return new AstyanaxTable(name, options, attributes, availability, read, write, dcSupplier);
    }

    private AstyanaxStorage newAstyanaxStorage(Storage storage, String table) {
        return newAstyanaxStorage(storage.getUuid(), storage.getShardsLog2(), storage.getReadsAllowed(),
                storage.getPlacement(), table);
    }

    private AstyanaxStorage newAstyanaxStorage(long uuid, int shardsLog2, boolean readsAllowed, final String placement,
                                               final String table) {
        // Delay resolving the placement until it's used so we can manipulate table metadata for placements that
        // don't replicate to the current data center.
        return new AstyanaxStorage(uuid, shardsLog2, readsAllowed, placement, Suppliers.memoize(new Supplier<Placement>() {
            @Override
            public Placement get() {
                try {
                    return _placementCache.get(placement);
                } catch (UnknownPlacementException e) {
                    // Add table information to the exception
                    e.setTable(table);
                    throw e;
                }
            }
        }));
    }

    private boolean isInternalPlacement(String placement) {
        return placement.endsWith(":sys");
    }

    private String checkPlacement(String placement) {
        if (!_placementFactory.isValidPlacement(placement)) {
            throw new IllegalArgumentException(format("Unknown placement string: %s", placement));
        }
        return placement;
    }

    private void checkPlacementConsistent(String placement, Instant since) {
        CassandraKeyspace keyspace = _placementCache.get(placement).getKeyspace();
        long lastConsistentAt = _fullConsistencyTimeProvider.getMaxTimeStamp(keyspace.getClusterName());
        if (lastConsistentAt <= since.toEpochMilli()) {
            throw new FullConsistencyException(placement);
        }
    }

    private Supplier<Collection<DataCenter>> getDataCentersSupplier(final String placement,
                                                                    @Nullable final String excludePlacement) {
        checkNotNull(placement, "placement");
        // Recompute the set of data centers each time since, rarely, new data centers may come online.
        return new Supplier<Collection<DataCenter>>() {
            @Override
            public Collection<DataCenter> get() {
                if (excludePlacement == null) {
                    return _placementFactory.getDataCenters(placement);
                } else {
                    return Sets.difference(
                            ImmutableSet.copyOf(_placementFactory.getDataCenters(placement)),
                            ImmutableSet.copyOf(_placementFactory.getDataCenters(excludePlacement)));
                }
            }
        };
    }

    /**
     * Returns a data center with local access to all specified placements using a deterministic algorithm that
     * always picks the same data center given the same set of placements.  Returns {@code null} if no data center
     * has access to all specified placements.
     */
    private String selectDataCenterForPlacements(String... placements) {
        // Get the set of data centers that have local access to all the placements.  Then pick one deterministically
        // (pick the one that sorts first alphabetically) and designate that one as the one to perform maintenance.
        Set<DataCenter> intersection = null;
        for (String placement : placements) {
            Set<DataCenter> dataCenters = Sets.newLinkedHashSet(_placementFactory.getDataCenters(placement));
            if (intersection == null) {
                intersection = dataCenters;
            } else {
                intersection.retainAll(dataCenters);
            }
        }
        if (intersection == null || intersection.isEmpty()) {
            return null;
        }
        return Ordering.natural().min(intersection).getName();
    }

    private String newTableUuidString(String table, Audit audit) {
        // This function assumes that it runs inside the global lock implemented by the ExclusiveTableDAO metadata mutex.
        for (; ; ) {
            long value = RANDOM.nextLong();

            // Never pick 0xffffffffffffffff since we can't use it in a range query for all rows between
            // (value, value+1) since value+1 overflows using unsigned math per the ByteOrderedPartitioner.
            if (value == -1) {
                continue; // pick another uuid
            }

            // Don't conflict with a bootstrap table.  Reserve table IDs that end in 0xffff for future system tables.
            if (_reservedUuids.contains(value) || (value & 0xffff) == 0xffff) {
                continue; // pick another uuid
            }

            String uuid = TableUuidFormat.encode(value);

            // 64-bit numbers are unique enough that we can assume uniqueness.  Check whether this uuid is in use.
            Map<String, Object> existing = _backingStore.get(_systemTableUuid, uuid, ReadConsistency.STRONG);
            if (!Intrinsic.isDeleted(existing)) {
                continue; // pick another uuid
            }

            // Nobody is using it.  Record that we're about it.  There's a small chance that the app crashes between
            // this write and the subsequent creation of the table with the uuid.  If that happens, we'll leak a uuid
            // but it should be harmless otherwise.
            Map<String, ?> json = ImmutableMap.of("table", table);
            _backingStore.update(_systemTableUuid, uuid, TimeUUIDs.newUUID(), Deltas.literal(json), audit, WriteConsistency.GLOBAL);

            return uuid;
        }
    }

    private Runnable rateLimited(Placement placement, final Runnable delegate) {
        final RateLimiter rateLimiter = _rateLimiterCache.get(placement.getKeyspace().getClusterName());
        return new Runnable() {
            @Override
            public void run() {
                rateLimiter.acquire();
                delegate.run();
            }
        };
    }

    @Override
    public void createStashTokenRangeSnapshot(String stashId, Set<String> placements, Condition blackListTableCondition) {
        checkState(_stashTableDao != null, "Call only valid in Stash mode");

        // Since we need to snapshot the TableJson call the backing store directly.
        final Iterator<Map<String, Object>> tableIter =
                _backingStore.scan(_systemTable, null, LimitCounter.max(), ReadConsistency.STRONG);

        while (tableIter.hasNext()) {
            TableJson tableJson = new TableJson(tableIter.next());
            Table table = tableFromJson(tableJson);
            if (table != null && table.getAvailability() != null) {
                AstyanaxStorage readStorage = ((AstyanaxTable) table).getReadStorage();
                String placementName = readStorage.getPlacement().getName();
                if (placements.contains(placementName)) {
                    // don't add the token ranges for the table mentioned in the blackList condition.
                    if (!isTableBlacklisted(table, blackListTableCondition)) {
                        _stashTableDao.addTokenRangesForTable(stashId, readStorage, tableJson);
                    }
                }
            }
        }
    }

    @Override
    public Iterator<StashTokenRange> getStashTokenRangesFromSnapshot(String stashId, String placement, ByteBuffer fromInclusive, ByteBuffer toExclusive) {
        checkState(_stashTableDao != null, "Call only valid in Stash mode");

        Iterator<CQLStashTableDAO.ProtoStashTokenRange> protoRanges = _stashTableDao.getTokenRangesBetween(stashId, placement, fromInclusive, toExclusive);

        // Convert the TableJson from the stash table DAO into Tables.
        return Iterators.transform(protoRanges, protoRange ->
                new StashTokenRange(protoRange.getFrom(), protoRange.getTo(), tableFromJson(protoRange.getTableJson())));
    }

    @Override
    public void clearStashTokenRangeSnapshot(String stashId) {
        checkState(_stashTableDao != null, "Call only valid in Stash mode");
        _stashTableDao.clearTokenRanges(stashId);
    }

    @Timed (name = "bv.emodb.table.AstyanaxTableDAO.listUnpublishedDatabusEvents", absolute = true)
    @Override
    public Iterator<UnpublishedDatabusEvent> listUnpublishedDatabusEvents(Date fromInclusive, Date toExclusive) {
        checkArgument(fromInclusive != null, "fromInclusive date cannot be null");
        checkArgument(toExclusive != null, "toExclusive date cannot be null");

        ZonedDateTime fromInclusiveUTC = fromInclusive.toInstant().atZone(ZoneOffset.UTC).with(ChronoField.MILLI_OF_DAY, 0);
        ZonedDateTime toInclusiveDateUTC = toExclusive.toInstant().atZone(ZoneOffset.UTC).with(ChronoField.MILLI_OF_DAY, 0);
        if (toExclusive.toInstant().equals(toInclusiveDateUTC.toInstant())) {
            toInclusiveDateUTC = toInclusiveDateUTC.minusDays(1);
        }

        // adding 1 to the days to make it inclusive.
        final int inclusiveNumberOfDays = (int) Duration.between(fromInclusiveUTC, toInclusiveDateUTC).toDays() + 1;
        if (inclusiveNumberOfDays > 31) {
            throw new IllegalArgumentException("Specified from and to date range is greater than 30 days which is not supported. Specify a smaller range.");
        }
        List<LocalDate> dates = Stream.iterate(fromInclusiveUTC.toLocalDate(), d -> d.plusDays(1))
                .limit(inclusiveNumberOfDays)
                .collect(Collectors.toList());

        List<UnpublishedDatabusEvent> allUnpublishedDatabusEvents = Lists.newArrayList();
        // using closedOpen which is [a..b) i.e. {x | a <= x < b}
        final Range<Long> requestedRange = Range.closedOpen(fromInclusive.getTime(), toExclusive.getTime());
        for (LocalDate date : dates) {
            String dateKey = date.toString();
            Map<String, Object> recordMap = _backingStore.get(_systemTableUnPublishedDatabusEvents, dateKey, ReadConsistency.STRONG);
            if (recordMap != null) {
                Object tableInfo = recordMap.get("tables");
                if (tableInfo != null) {
                    List<UnpublishedDatabusEvent> unpublishedDatabusEvents = JsonHelper.convert(tableInfo, new TypeReference<List<UnpublishedDatabusEvent>>() {});
                    // filter events whose time is outside the requestedRange.
                    unpublishedDatabusEvents.stream()
                            .filter(unpublishedDatabusEvent -> requestedRange.contains(unpublishedDatabusEvent.getDate().getTime()))
                            .forEach(allUnpublishedDatabusEvents::add);
                }
            }
        }

        return allUnpublishedDatabusEvents.iterator();
    }

    @Override
    public void writeUnpublishedDatabusEvent(String name, UnpublishedDatabusEventType attribute) {
        checkNotNull(name, "table");

        ZonedDateTime dateTime = ZonedDateTime.ofInstant(_clock.instant(), ZoneOffset.UTC);
        String date = dateTime.toLocalDate().toString();

        // Forcing millisecond precision for the Zoned date time value,
        // as zero milli second or second case omission will lead to parsing errors when deserializing the UnpublishedDatabusEvents POJO.
        Delta delta = newUnpublishedDatabusEventUpdate(name, attribute.toString(), getMillisecondPrecisionZonedDateTime(dateTime).toString());
        Audit augmentedAudit = new AuditBuilder()
                .set("_unpublished-databus-event-update", attribute.toString())
                .build();

        writEventToSystemTable(date, delta, augmentedAudit);
    }

    // Delta for storing the unpublished databus events.
    Delta newUnpublishedDatabusEventUpdate(String tableName, String updateType, String datetime) {
        return Deltas.mapBuilder()
                .update("tables", Deltas.setBuilder().add(ImmutableMap.of("table", tableName, "date", datetime, "event", updateType)).build())
                .build();
    }

    @VisibleForTesting
    static boolean isTableBlacklisted(Table table, Condition blackListTableCondition) {
        Map<String, Object> tableAttributes = table.getAttributes();
        return ConditionEvaluator.eval(blackListTableCondition, tableAttributes, new TableFilterIntrinsics(table));
    }

    /*
       The output of toString From ZonedDateTime will be one of the following ISO-8601 formats:
       uuuu-MM-dd'T'HH:mm
       uuuu-MM-dd'T'HH:mm:ss
       uuuu-MM-dd'T'HH:mm:ss.SSS
       uuuu-MM-dd'T'HH:mm:ss.SSSSSS
       uuuu-MM-dd'T'HH:mm:ss.SSSSSSSSS
       Note that the format used will be the shortest that outputs the full value of the time where the omitted parts are implied to be zero.

       This helper method will force the Seconds and Milliseconds precision when serializing ZonedDateTime.
       for example:
       2017-01-01T07:01Z will always be serialized as 2017-01-01T07:01:00.000Z
       2017-01-01T07:01:01Z will always be serialized as 2017-01-01T07:01:01.000Z
     */
    @VisibleForTesting
    protected static String getMillisecondPrecisionZonedDateTime(ZonedDateTime dateTime) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX")
                .withZone(ZoneId.of("UTC"))
                .format(dateTime);
    }
}