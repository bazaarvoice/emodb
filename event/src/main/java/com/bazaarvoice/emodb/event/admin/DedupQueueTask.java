package com.bazaarvoice.emodb.event.admin;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.event.core.MetricsGroupName;
import com.bazaarvoice.emodb.event.dedup.DedupQueue;
import com.bazaarvoice.emodb.event.dedup.DedupQueueAdmin;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Dropwizard task for inspecting the internals of active {@link DedupQueue} instances.
 * <p>
 * Usage:
 * <pre>
 *     # View the current state of all active DedupQueue objects that have won the leadership election:
 *     curl -s -XPOST http://localhost:8081/tasks/dedup-databus
 *     curl -s -XPOST http://localhost:8081/tasks/dedup-queue
 *
 *     # View a single DedupQueue object.  This may be useful when estimating the size of *all* the
 *     # queues is slow and you're only interested in one:
 *     curl -s -XPOST http://localhost:8081/tasks/dedup-databus?print=my-subscription
 *     curl -s -XPOST http://localhost:8081/tasks/dedup-queue?print=my-queue
 * </pre>
 */
public class DedupQueueTask extends Task {
    private final DedupQueueAdmin _admin;

    @Inject
    public DedupQueueTask(@MetricsGroupName String metricsGroup, DedupQueueAdmin admin, TaskRegistry tasks) {
        super("dedup-" + metricsGroup.substring(metricsGroup.lastIndexOf('.') + 1));
        _admin = checkNotNull(admin, "_admin");
        tasks.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter out) {
        for (String queue : parameters.get("activate")) {
            boolean activated = _admin.activateQueue(queue);
            out.printf("Activation %s: %s%n", activated ? "succeeded" : "FAILED", queue);
        }

        // Print a summary of high-level queue statistics.
        Map<String, DedupQueue> activeMap = _admin.getActiveQueues();
        Collection<String> queues = parameters.containsKey("print") ? parameters.get("print") :
                Ordering.natural().immutableSortedCopy(activeMap.keySet());

        if (queues.isEmpty()) {
            out.println("no active queues");
            return;
        }

        for (String name : queues) {
            DedupQueue queue = activeMap.get(name);
            if (queue != null) {
                out.println(queue);
                out.flush();
            }
        }
    }
}
