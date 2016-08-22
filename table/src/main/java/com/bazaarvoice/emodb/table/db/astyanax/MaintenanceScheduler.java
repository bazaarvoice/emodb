package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationEvent;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationListener;
import com.bazaarvoice.emodb.table.db.Mutex;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Schedules and dispatches table maintenance tasks in the current data center.
 * <p>
 * This class is responsible for ensuring that maintenance is executed in the correct data center and with the
 * correct system-wide locks.  It's also responsible for sending cluster-wide cache invalidation requests etc.
 * which trigger sequencing of the maintenance operations.
 */
public class MaintenanceScheduler extends AbstractIdleService implements InvalidationListener {
    private static final Logger _log = LoggerFactory.getLogger(MaintenanceScheduler.class);

    private static final Duration ACQUIRE_TIMEOUT = Duration.standardMinutes(5);

    /**
     * When maintenance fails, wait an hour before trying again.  We're generally not in a big hurry for maintenance
     * to complete and an hour is long enough that the underlying failure cause may have cleared up (eg. high load,
     * data center partition) and it's slow enough that we shouldn't spam the logs with zillions of exceptions.
     */
    private static final Duration RETRY_DELAY = Duration.standardHours(1);

    private static final ThreadFactory _threadFactory =
            new ThreadFactoryBuilder().setNameFormat("TableMaintenance-%d").build();

    private final MaintenanceDAO _maintDao;
    private final Optional<Mutex> _metadataMutex;
    private final String _selfDataCenter;
    private final ScheduledExecutorService _executor = Executors.newSingleThreadScheduledExecutor(_threadFactory);
    private final CacheHandle _tableCacheHandle;
    private final Map<String, Task> _scheduledTasks = Maps.newHashMap();  // By table name
    private Task _runningTask;

    public MaintenanceScheduler(MaintenanceDAO maintenanceDao, Optional<Mutex> metadataMutex, String selfDataCenter,
                                CacheRegistry cacheRegistry, MoveTableTask task) {
        _maintDao = checkNotNull(maintenanceDao, "maintenanceDao");
        _metadataMutex = checkNotNull(metadataMutex, "metadataMutex");
        _selfDataCenter = checkNotNull(selfDataCenter, "selfDataCenter");
        _tableCacheHandle = cacheRegistry.lookup("tables", true);
        cacheRegistry.addListener(this);
        task.setScheduler(this);
    }

    @Override
    protected void startUp() {
        scheduleAll();
    }

    @Override
    protected void shutDown() {
        _executor.shutdownNow();
    }

    @Override
    public void handleInvalidation(final InvalidationEvent event) {
        if (!_tableCacheHandle.matches(event) || !isRunningOrStarting()) {
            return;
        }
        // Don't read table metadata from the invalidation thread to avoid holding up other important operations.  Use
        // the scheduled executor thread since there's only one thread, don't have to worry much about race conditions.
        _executor.submit(new Runnable() {
            @Override
            public void run() {
                if (event.hasKeys()) {
                    for (String name : event.getKeys()) {
                        scheduleTable(name);
                    }
                } else {
                    scheduleAll();
                }
            }
        });
    }

    private void scheduleTable(String name) {
        scheduleTask(name, _maintDao.getNextMaintenanceOp(name));
    }

    private void scheduleAll() {
        Iterator<Map.Entry<String, MaintenanceOp>> iter = _maintDao.listMaintenanceOps();
        while (iter.hasNext()) {
            Map.Entry<String, MaintenanceOp> entry = iter.next();
            scheduleTask(entry.getKey(), entry.getValue());
        }
    }

    private synchronized void scheduleTask(String table, MaintenanceOp op) {
        if (op == null || !mayPerformMaintenance(op) || !isRunningOrStarting()) {
            return;
        }

        Task existing = _scheduledTasks.get(table);
        if (existing != null) {
            if (existing.op.getWhen().equals(op.getWhen())) {
                existing.op = op;
                return;  // Future is already scheduled at the desired time.
            }
            existing.future.cancel(false);
            _scheduledTasks.remove(table);
        }

        final Task task = new Task(table, op);
        task.future = _executor.schedule(new Runnable() {
            @Override
            public void run() {
                startTask(task);
                try {
                    if (isRunningOrStarting()) {
                        performMaintenance(task.table, task.op);
                    }
                } finally {
                    finishTask();
                }
            }
        }, Math.max(0, op.getWhen().getMillis() - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
        _scheduledTasks.put(table, task);
    }

    private synchronized void startTask(Task task) {
        // Remove from '_scheduledTasks' so a concurrent call to scheduleTask() won't modify the Task object.
        if (_scheduledTasks.get(task.table) == task) {
            _scheduledTasks.remove(task.table);
        }
        _runningTask = task;
    }

    private synchronized void finishTask() {
        _runningTask = null;
    }

    synchronized Map<String, MaintenanceOp> getScheduledMaintenance() {
        // Make a copy so we don't have to worry about concurrent access.
        return Maps.newHashMap(Maps.transformValues(_scheduledTasks, new Function<Task, MaintenanceOp>() {
            @Override
            public MaintenanceOp apply(Task task) {
                return task.op;
            }
        }));
    }

    @Nullable
    synchronized Map.Entry<String, MaintenanceOp> getRunningMaintenance() {
        return _runningTask != null ? Maps.immutableEntry(_runningTask.table, _runningTask.op) : null;
    }

    private boolean mayPerformMaintenance(MaintenanceOp op) {
        switch (op.getType()) {
            case METADATA:
                return _metadataMutex.isPresent();
            case DATA:
                return _selfDataCenter.equals(op.getDataCenter());
            default:
                return false;
        }
    }

    private void performMaintenance(String table, MaintenanceOp op) {
        // Set the thread name so we can tell in stack dumps what this thread is working on
        Thread thread = Thread.currentThread();
        String oldThreadName = thread.getName();
        boolean reschedule = true;
        try {
            thread.setName(String.format("%s - %s - %s", oldThreadName, op.getName(), table));
            switch (op.getType()) {
                case METADATA:
                    performMetadataMaintenance(table);
                    break;
                case DATA:
                    performDataMaintenance(table);
                    break;
            }
            reschedule = false;
        } catch (FullConsistencyException t) {
            // This is handled the same as any other exception thrown by maintenance except that it logs less loudly.
            // In local testing where the hints poller isn't present force full consistency with the following:
            //  curl -s -XPOST "localhost:8081/tasks/compaction-timestamp?all=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
            //  curl -s -XPOST 'localhost:8081/tasks/compaction-lag?all=PT0.001S'
            _log.info("Waiting for full consistency before proceeding with '{}' maintenance on table: {}, {}",
                    op.getName(), table, t.getMessage());
        } catch (Throwable t) {
            _log.error("Unexpected exception performing '{}' maintenance on table: {}", op.getName(), table, t);
        } finally {
            thread.setName(oldThreadName);
        }
        if (reschedule) {
            scheduleTask(table, MaintenanceOp.reschedule(op, new DateTime().plus(RETRY_DELAY)));
        }
    }

    private void performMetadataMaintenance(final String table) {
        _metadataMutex.get().runWithLock(new Runnable() {
            @Override
            public void run() {
                _maintDao.performMetadataMaintenance(table);
            }
        }, ACQUIRE_TIMEOUT);
    }

    private void performDataMaintenance(final String table) {
        _maintDao.performDataMaintenance(table, new Runnable() {
            @Override
            public void run() {
                _log.debug("Making progress on {}...", table);
                checkState(isRunningOrStarting(), "Maintenance scheduler has lost leadership.");
            }
        });
    }

    boolean isRunningOrStarting() {
        return isRunning() || state() == State.STARTING;
    }

    /** Keeps track of pending maintenance actions.  Some fields are mutable until the maintenance operation begins. */
    private static class Task {
        final String table;
        MaintenanceOp op;
        ScheduledFuture<?> future;

        private Task(String table, MaintenanceOp op) {
            this.table = table;
            this.op = op;
        }
    }
}
