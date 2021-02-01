package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.table.db.TableChangesEnabled;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.servlets.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Dropwizard task for administratively disabling all operations that modify table metadata.  The enabled setting is
 * stored in ZooKeeper and affects all EmoDB servers in the same environment and data center, and it persists across
 * restarts. You can use this task when cross-region Cassandra replication is down to preemptively disable table
 * operations that require EACH_QUORUM consistency and are therefore guaranteed to fail.
 * <p>
 * For example, this disallows the following:
 * <ul>
 *     <li>Table create</li>
 *     <li>Table drop</li>
 *     <li>Table move</li>
 *     <li>Table change attributes</li>
 *     <li>Facade create</li>
 *     <li>Facade drop</li>
 *     <li>Facade move</li>
 *     <li>Background table maintenance</li>
 * </ul>
 *
 * Usage:
 * <pre>
 * # Print Status, Disable, Enable for System of Record
 * curl -s -XPOST http://localhost:8081/tasks/sor-table-changes
 * curl -s -XPOST http://localhost:8081/tasks/sor-table-changes?enabled=false
 * curl -s -XPOST http://localhost:8081/tasks/sor-table-changes?enabled=true
 *
 * # Print Status, Disable, Enable for Blob Store
 * curl -s -XPOST http://localhost:8081/tasks/blob-table-changes
 * curl -s -XPOST http://localhost:8081/tasks/blob-table-changes?enabled=false
 * curl -s -XPOST http://localhost:8081/tasks/blob-table-changes?enabled=true
 * </pre>
 */
public class TableChangesEnabledTask extends Task {
    private static final Logger _log = LoggerFactory.getLogger(TableChangesEnabledTask.class);

    private final ValueStore<Boolean> _enabled;

    @Inject
    public TableChangesEnabledTask(TaskRegistry tasks, LifeCycleRegistry lifeCycle, @Maintenance final String scope,
                                   @TableChangesEnabled ValueStore<Boolean> enabled) {
        super(scope + "-table-changes");
        _enabled = checkNotNull(enabled, "enabled");
        tasks.addTask(this);

        // Default is enabled, so at startup warn if disabled since otherwise essential functionality won't work.
        lifeCycle.manage(new Managed() {
            @Override
            public void start() throws Exception {
                if (!_enabled.get()) {
                    _log.warn("({}) Table create/drop/update operations and table maintenance are: DISABLED", scope);
                }
            }

            @Override
            public void stop() throws Exception {
            }
        });
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter out) throws Exception {
        for (String enabled : parameters.get("enabled")) {
            _enabled.set(parseBoolean(enabled));

            Thread.sleep(500);  // Wait for values to round trip through ZooKeeper.
        }

        out.printf("Table create/drop/update operations and table maintenance are: %s%n",
                _enabled.get() ? "ENABLED" : "DISABLED");
    }

    private static Boolean parseBoolean(String input) throws Exception {
        if ("true".equalsIgnoreCase(input)) {
            return Boolean.TRUE;
        } else if ("false".equalsIgnoreCase(input)) {
            return Boolean.FALSE;
        } else {
            throw new Exception('"' + input + "\" must be \"true\" or \"false\".");
        }
    }
}
