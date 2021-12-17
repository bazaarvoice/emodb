package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.event.DedupEnabled;
import com.bazaarvoice.emodb.event.api.DedupEventStore;
import com.bazaarvoice.emodb.sortedq.core.ReadOnlyQueueException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.servlets.tasks.Task;
import io.dropwizard.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Collection;

import static java.util.Objects.requireNonNull;

/**
 * Disables dedup queues and reverts to using non-dedup'd queues for the Databus.
 * <p>
 * Reverting is a multi-step process:
 * <pre>
 *  # Disable using the dedup queues for the Databus (disabled flag is saved cluster-wide in ZooKeeper)
 *  curl -s -XPOST &lt;emodb-host1>:8081/tasks/dedup-databus-migration?dedup=false
 *
 *  # Wait a little while for all the servers to react and for requests in-flight to complete.
 *  sleep 30
 *
 *  # For each emodb server, migrate the data in the queues managed by that server back to non-dedup'd queues:
 *  for host in &lt;emodb-host1> &lt;emodb-host2> ... ; do
 *    curl -s -XPOST ${host}:8081/tasks/dedup-databus-migration?migrate=all
 *  done
 * </pre>
 * To re-enable dedup queues for the databus:
 * <pre>
 *  curl -s -XPOST http://localhost:8081/tasks/dedup-databus-migration?dedup=true
 * </pre>
 */
public class DedupMigrationTask extends Task {
    private static final Logger _log = LoggerFactory.getLogger(DedupMigrationTask.class);

    private final DedupEventStore _eventStore;
    private final ValueStore<Boolean> _dedupEnabled;

    @Inject
    public DedupMigrationTask(TaskRegistry tasks, LifeCycleRegistry lifeCycle, DedupEventStore eventStore,
                              @DedupEnabled ValueStore<Boolean> dedupEnabled) {
        super("dedup-databus-migration");
        _eventStore = requireNonNull(eventStore, "eventStore");
        _dedupEnabled = requireNonNull(dedupEnabled, "dedupEnabled");
        tasks.addTask(this);

        // Default is enabled, so at startup warn if disabled since otherwise essential functionality won't work.
        lifeCycle.manage(new Managed() {
            @Override
            public void start() throws Exception {
                if (!_dedupEnabled.get()) {
                    _log.warn("Databus deduplication is: DISABLED");
                }
            }

            @Override
            public void stop() throws Exception {
            }
        });
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter out) throws Exception {
        boolean oldEnabled = _dedupEnabled.get();
        boolean newEnabled = oldEnabled;

        for (String value : parameters.get("dedup")) {
            newEnabled = Boolean.parseBoolean(value);
            _dedupEnabled.set(newEnabled);
        }

        out.printf("dedup-enabled: %s%n", newEnabled);

        Collection<String> migrations = parameters.get("migrate");
        if (!migrations.isEmpty()) {
            if (newEnabled) {
                out.println("Ignoring migrations since Databus dedup is still enabled.");
            } else {
                if (oldEnabled) {
                    out.println("Sleeping 15 seconds to allow in-flight requests to complete.");
                    out.flush();
                    Thread.sleep(Duration.seconds(15).toMilliseconds());
                }
                migrate(migrations, out);
            }
        }
    }

    private void migrate(Collection<String> migrations, PrintWriter out) {
        // When migrating back to non-dedup queues you must migrate all channels for correctness.  But sometimes
        // it's useful during debugging to just migrate a single queue, or you can sequence the migration to move
        // some queues before others.
        for (String migrate : migrations) {
            if ("all".equalsIgnoreCase(migrate)) {
                for (String queue : ImmutableList.copyOf(_eventStore.listChannels())) {
                    migrate(queue, out);
                }
            } else {
                migrate(migrate, out);
            }
        }
    }

    private void migrate(String queue, PrintWriter out) {
        if (ChannelNames.isNonDeduped(queue)) {
            return;
        }

        out.printf("migrating queue '%s'...", queue);
        out.flush();

        try {
            _eventStore.moveToRawChannel(queue, queue);
            out.println(" COMPLETE");
        } catch (ReadOnlyQueueException e) {
            out.println(" skipped");
        } catch (Exception e) {
            out.printf(" FAILED: %s%n", e);
        }
    }
}
