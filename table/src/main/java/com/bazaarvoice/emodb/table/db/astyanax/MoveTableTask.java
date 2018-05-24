package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.FacadeOptions;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.table.db.MoveType;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

/**
 * Shows the current scheduled table maintenance (for move and drop operations) and allows scheduling table moves,
 * changing placement and #shards.
 * <p>
 * Invoke this task as follows to see what maintenance is pending locally and across the cluster:
 * <pre>
 *   curl -s -XPOST http://localhost:8081/tasks/sor-move
 *   curl -s -XPOST http://localhost:8081/tasks/blob-move
 * </pre>
 * <p>
 * To move a table to a new placement execute something like this:
 * <pre>
 *   curl -s -XPOST "http://localhost:8081/tasks/sor-move?table=review:testcustomer&dest=ugc_global:ugc"
 * </pre>
 * To change the number of shards for a facade execute something like this (note "src" is required for facades to
 * identify which facade to move, and "dest" placement is required even though the placement isn't changing):
 * <pre>
 *   curl -s -XPOST "http://localhost:8081/tasks/sor-move?facade=review:testcustomer&src=ugc_us:ugc&dest=ugc_us:ugc&shards=16"
 * </pre>
 * To move a whole table placement to a new placement execute something like this:
 * <pre>
 *   curl -s -XPOST "http://localhost:8081/tasks/blob-move?placement=media_global:ugc&dest=media_global:media"
 * </pre>
 * In practice, this would be used when separating a placement to its own dedicated Cassandra cluster
 * </p>
 * Query parameters:
 * <dl>
 *     <dt>table</dt><dd>Zero or more tables to move.</dd>
 *     <dt>facade</dt><dd>Zero or more facades to move.</dd>
 *     <dt>src</dt><dd>Source placement when moving facades.</dd>
 *     <dt>dest</dt><dd>Destination placement when moving tables and/or facades.</dd>
 *     <dt>shards</dt><dd>Optional number of shards to use for the destination, if specified overrides the normal
 *     default.  This must be a power of two between 1 and 256.</dd>
 *     <dt>localonly</dt><dd>Boolean (true|false) specifies whether to show maintenance for the entire cluster (which
 *     can be slow to compute) or just maintenance scheduled on the local node.</dd>
 * </dl>
 */
public class MoveTableTask extends Task {
    private final TableDAO _tableDao;
    private final Map<String, String> _placementsUnderMove;
    private final MaintenanceDAO _maintDao;
    private volatile Reference<MaintenanceScheduler> _scheduler = new WeakReference<>(null);

    @Inject
    public MoveTableTask(TaskRegistry taskRegistry, @Maintenance String scope,
                         TableDAO tableDao, MaintenanceDAO maintDao,
                         @PlacementsUnderMove Map<String, String> placementsUnderMove) {
        super(scope + "-move");
        _tableDao = checkNotNull(tableDao, "tableDAO");
        _maintDao = checkNotNull(maintDao, "maintDao");
        _placementsUnderMove = checkNotNull(placementsUnderMove, "placementsUnderMove");
        taskRegistry.addTask(this);
    }

    void setScheduler(MaintenanceScheduler scheduler) {
        // Called when the scheduler wins the leadership election.  Holds a weak reference so we don't have
        // to watch for when leadership is lost.
        _scheduler = new WeakReference<>(scheduler);
    }

    @Nullable
    private MaintenanceScheduler getMaintenanceScheduler() {
        MaintenanceScheduler scheduler = _scheduler.get();
        return scheduler != null && scheduler.isRunningOrStarting() ? scheduler : null;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter out) throws Exception {
        // Start table and facade moves as specified in query parameters.
        String srcPlacement = Iterables.getFirst(parameters.get("src"), null);
        String destPlacement = Iterables.getFirst(parameters.get("dest"), null);
        String movePlacement = Iterables.getFirst(parameters.get("placement"), null);
        Optional<Integer> numShards = parseNumShards(Iterables.getFirst(parameters.get("shards"), null));
        checkArgument(destPlacement == null || _tableDao.isMoveToThisPlacementAllowed(destPlacement),
                "Move to this placement is not allowed since it is currently on move.");
        moveTables(parameters.get("table"), destPlacement, numShards, out, MoveType.SINGLE_TABLE);
        moveFacades(parameters.get("facade"), srcPlacement, destPlacement, numShards, out, MoveType.SINGLE_TABLE);
        movePlacement(movePlacement, destPlacement, numShards, out);

        // Print currently scheduled table maintenance.
        boolean localOnly = Boolean.valueOf(Iterables.getFirst(parameters.get("localonly"), "false"));
        Map.Entry<String, MaintenanceOp> running = getLocallyRunningMaintenance();
        Map<String, MaintenanceOp> localMap = getLocallyScheduledMaintenance();
        Map<String, MaintenanceOp> remoteMap = Collections.emptyMap();
        if (!localOnly) {
            // Remote maintenance == ALL - LOCAL - RUNNING.
            remoteMap = Maps.filterEntries(
                    Maps.difference(getGloballyScheduledMaintenance(), localMap).entriesOnlyOnLeft(),
                    Predicates.not(Predicates.equalTo(running)));
        }
        if (running != null) {
            printMaintenance(running.getKey(), running.getValue(), "RUNNING", out);
        }
        printMaintenance(localMap, "LOCAL", out);
        printMaintenance(remoteMap, "REMOTE", out);
        if (running == null && localMap.isEmpty() && remoteMap.isEmpty()) {
            if (localOnly) {
                out.println("No local table maintenance is scheduled.");
            } else {
                out.println("No table maintenance is scheduled.");
            }
        }
    }

    private void printMaintenance(Map<String, MaintenanceOp> map, String state, PrintWriter out) {
        // Sort the maintenance operations by date then table name.
        List<Map.Entry<String, MaintenanceOp>> entries = new Ordering<Map.Entry<String, MaintenanceOp>>() {
            @Override
            public int compare(Map.Entry<String, MaintenanceOp> left, Map.Entry<String, MaintenanceOp> right) {
                return ComparisonChain.start()
                        .compare(left.getValue(), right.getValue())
                        .compare(left.getKey(), right.getKey())
                        .result();
            }
        }.immutableSortedCopy(map.entrySet());
        for (Map.Entry<String, MaintenanceOp> entry : entries) {
            printMaintenance(entry.getKey(), entry.getValue(), state, out);
        }
    }

    private void printMaintenance(String table, MaintenanceOp op, String state, PrintWriter out) {
        out.printf("[%s] %s: type=%s  dc=%s  op=%s  table=%s%n", state,
                JsonHelper.formatTimestamp(op.getWhen().toEpochMilli()),
                op.getType(), op.getDataCenter(), op.getName(), table);
    }

    private Map.Entry<String, MaintenanceOp> getLocallyRunningMaintenance() {
        MaintenanceScheduler scheduler = getMaintenanceScheduler();
        return scheduler != null ? scheduler.getRunningMaintenance() : null;
    }

    private Map<String, MaintenanceOp> getLocallyScheduledMaintenance() {
        MaintenanceScheduler scheduler = getMaintenanceScheduler();
        return scheduler != null ? scheduler.getScheduledMaintenance() : Collections.<String, MaintenanceOp>emptyMap();
    }

    private Map<String, MaintenanceOp> getGloballyScheduledMaintenance() {
        return toMap(_maintDao.listMaintenanceOps());
    }

    private void moveTables(Collection<String> tables, String destPlacement,
                            Optional<Integer> numShards, PrintWriter out, MoveType moveType) {
        if (!tables.isEmpty() && destPlacement == null) {
            out.println("The 'dest' placement query parameter is required when moving tables.");
            return;
        }
        for (String table : tables) {
            out.printf("Moving table %s to placement %s...%n", table, destPlacement);
            try {
                _tableDao.move(table, destPlacement, numShards, new AuditBuilder().build(), moveType);
            } catch (Exception e) {
                out.printf("ERROR moving table %s to placement %s: ", table, destPlacement);
                e.printStackTrace(out);
            }
        }
    }

    private void moveFacades(Collection<String> facades, String srcPlacement, String destPlacement,
                             Optional<Integer> numShards, PrintWriter out, MoveType moveType) {
        if (!facades.isEmpty() && (srcPlacement == null || destPlacement == null)) {
            out.println("The 'src' and 'dest' placement query parameters are required when moving facades.");
            return;
        }
        for (String facade : facades) {
            out.printf("Moving facade %s to placement %s...%n", facade, destPlacement);
            try {
                _tableDao.moveFacade(facade, srcPlacement, destPlacement, numShards, new AuditBuilder().build(), moveType);
            } catch (Exception e) {
                out.printf("ERROR moving facade %s to placement %s: ", facade, destPlacement);
                e.printStackTrace(out);
            }
        }
    }

    private void movePlacement(String placement, String destPlacement,
                               Optional<Integer> numShards, PrintWriter out) {
        if (placement == null) {
            return;
        }
        if (destPlacement == null) {
            out.println("The 'dest' placement query parameter is required when moving placement.");
            return;
        }
        if (!Objects.equal(_placementsUnderMove.get(placement), destPlacement)) {
            out.println("The 'dest' placement should be configured as destination for the source placement");
            return;
        }
        MovePlacement movePlacement = getTablesAndFacadesInSrcPlacement(placement);

        moveTables(movePlacement.getTables(), destPlacement, numShards, out, MoveType.FULL_PLACEMENT);
        moveFacades(movePlacement.getFacades(), placement, destPlacement, numShards, out, MoveType.FULL_PLACEMENT);
    }

    private Optional<Integer> parseNumShards(String numShards) {
        return Optional.fromNullable(numShards != null ? Integer.parseInt(numShards) : null);
    }

    private <K, V> Map<K, V> toMap(Iterator<Map.Entry<K, V>> iter) {
        Map<K, V> map = Maps.newLinkedHashMap();
        while (iter.hasNext()) {
            Map.Entry<K, V> entry = iter.next();
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    @VisibleForTesting
    protected MovePlacement getTablesAndFacadesInSrcPlacement(String placement) {
        MovePlacement movePlacement = new MovePlacement();
        // Get all tables for the placement
        Iterator<Table> allTables = _tableDao.list(null, new LimitCounter(12345678));
        while (allTables.hasNext()) {
            Table table = allTables.next();

            // Make sure there are no tables that are currently moving into the placement.
            String readPlacement = ((AstyanaxTable) table).getReadStorage().getPlacementName();
            for(AstyanaxStorage storage : ((AstyanaxTable) table).getWriteStorage()) {
                String writePlacement = storage.getPlacementName();
                checkState(readPlacement.equals(writePlacement) ||
                                !writePlacement.equals(placement),
                        format("%s table is moving into the source placement. " +
                                "Can't move placement if tables are moving into it.", table.getName()));
            }

            TableOptions options = table.getOptions();
            // See if its master placement or any of its facades match the source placement
            if (options.getPlacement().equals(placement)) {
                movePlacement.getTables().add(table.getName());
            } else {
                // See if any of the facades match the source placement
                for (FacadeOptions facadeOptions : options.getFacades()) {
                    if (facadeOptions.getPlacement().equals(placement)) {
                        movePlacement.getFacades().add(table.getName());
                    }
                }
            }
        }
        return movePlacement;
    }

    @VisibleForTesting
    public class MovePlacement {
        private Set<String> _tables = Sets.newHashSet();
        private Set<String> _facades = Sets.newHashSet();

        public Set<String> getTables() {
            return _tables;
        }

        public Set<String> getFacades() {
            return _facades;
        }
    }
}
