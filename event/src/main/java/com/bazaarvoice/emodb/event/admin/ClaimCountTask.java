package com.bazaarvoice.emodb.event.admin;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.event.api.EventStore;
import com.bazaarvoice.emodb.event.core.MetricsGroupName;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Reports the number of QueueService or Databus claims.  This is useful for tracking client activity, determining
 * which queues are busy and which clients are ack'ing in a timely way and which are not.
 * <p>
 * Usage:
 * <pre>
 * curl -s -XPOST http://localhost:8081/tasks/claims-databus
 * curl -s -XPOST http://localhost:8081/tasks/claims-queue
 * </pre>
 */
public class ClaimCountTask extends Task {

    private final EventStore _eventStore;

    @Inject
    public ClaimCountTask(TaskRegistry tasks, @MetricsGroupName String metricsGroup, EventStore eventStore) {
        super("claims-" + metricsGroup.substring(metricsGroup.lastIndexOf('.') + 1));
        _eventStore = checkNotNull(eventStore, "eventStore");
        tasks.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        Map<String, Long> map = _eventStore.snapshotClaimCounts();

        if (map.isEmpty()) {
            output.println("no claims");
            return;
        }

        // Sort the entries in the map from biggest to smallest.
        List<Map.Entry<String,Long>> entries = Lists.newArrayList(map.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> e1, Map.Entry<String, Long> e2) {
                return ComparisonChain.start()
                        .compare(e2.getValue(), e1.getValue())   // Sort counts descending
                        .compare(e1.getKey(), e2.getKey())       // Sort channel names ascending
                        .result();
            }
        });

        // Print a formatted table with two columns.
        for (Map.Entry<String, Long> entry : entries) {
            output.println(Strings.padStart(entry.getValue().toString(), 9, ' ') + "  " + entry.getKey());
        }
    }
}
