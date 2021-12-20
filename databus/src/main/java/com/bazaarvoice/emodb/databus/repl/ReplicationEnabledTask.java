package com.bazaarvoice.emodb.databus.repl;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.databus.ReplicationEnabled;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.servlets.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;

import static java.util.Objects.requireNonNull;

/**
 * Dropwizard task for administratively disabling cross-region Databus replication.  When disabled, databus events
 * are not copied from one region to another, so writes in one data center do not trigger databus events in other
 * data centers.  Events are not lost--they are held in a queue and delivered once replication is re-enabled.
 * You can use this task when cross-region Cassandra replication is down to avoid having remote regions attempt
 * to find and deliver updates for SoR deltas that have not yet been replicated, causing a lot of wasted polling.
 * <pre>
 * # Print Status, Disable, Enable
 * curl -s -XPOST http://localhost:8081/tasks/busrepl
 * curl -s -XPOST http://localhost:8081/tasks/busrepl?enabled=false
 * curl -s -XPOST http://localhost:8081/tasks/busrepl?enabled=true
 * </pre>
 */
public class ReplicationEnabledTask extends Task {
    private static final Logger _log = LoggerFactory.getLogger(ReplicationEnabledTask.class);

    private final ValueStore<Boolean> _enabled;

    @Inject
    public ReplicationEnabledTask(TaskRegistry tasks, LifeCycleRegistry lifeCycle,
                                  @ReplicationEnabled ValueStore<Boolean> enabled) {
        super("busrepl");
        _enabled = requireNonNull(enabled, "enabled");
        tasks.addTask(this);

        // Default is enabled, so warn if disabled since otherwise essential functionality won't work.
        lifeCycle.manage(new Managed() {
            @Override
            public void start() throws Exception {
                if (!_enabled.get()) {
                    _log.warn("Databus inbound event replication from other data centers is: DISABLED");
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

        out.printf("Databus inbound event replication from other data centers is: %s%n",
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
