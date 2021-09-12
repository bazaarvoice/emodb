package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.DataCenterFanoutPartitions;
import com.bazaarvoice.emodb.databus.MasterFanoutPartitions;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.event.api.EventStore;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Task which can be used to drain a fanout partition if the number of partitions is ever decreased from a higher to
 * a lower value.  For example, if the number of master fanout partitions is decreased from 6 to 4 then this task
 * must be used to fully drain the partitions 4 and 5 to one of the remaining partitions, [0..3].  This task should
 * only be run after all hosts with the old number of partitions have been shut down, otherwise new events may be
 * added to the partitions after they have been drained.
 *
 * Examples:
 *
 * Drain the master queue from partition 4 to partition 3:
 * <pre>
 *  curl -s -XPOST "http://localhost:8081/tasks/drain-fanout-partition?fanout=master&from=4&to=3"
 * </pre>
 *
 * Drain the outbound replication queue for eu-west-1 from partition 2 to partition 1:
 * <pre>
 *  curl -s -XPOST "http://localhost:8081/tasks/drain-fanout-partition?fanout=eu-west-1&from=2&to=1"
 * </pre>
 */
public class DrainFanoutPartitionTask extends Task {

    private final EventStore _eventStore;
    private final int _masterFanoutPartitions;
    private final int _dataCenterFanoutPartitions;
    private final DataCenters _dataCenters;

    @Inject
    public DrainFanoutPartitionTask(TaskRegistry tasks,
                                    EventStore eventStore,
                                    @MasterFanoutPartitions int masterFanoutPartitions,
                                    @DataCenterFanoutPartitions int dataCenterFanoutPartitions,
                                    DataCenters dataCenters) {
        super("drain-fanout-partition");
        _eventStore = eventStore;
        _masterFanoutPartitions = masterFanoutPartitions;
        _dataCenterFanoutPartitions = dataCenterFanoutPartitions;
        _dataCenters = dataCenters;

        tasks.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter printWriter) throws Exception {
        String fanout = getOnlyParameter("fanout", parameters);
        if (fanout == null) {
            printWriter.write("Exactly one \"fanout\" parameter is required");
            return;
        }

        String fromString = getOnlyParameter("from", parameters);
        if (fromString == null) {
            printWriter.write("Exactly one \"from\" parameter is required");
            return;
        }
        int from = Integer.parseInt(fromString);
        if (from < 0) {
            printWriter.write("Invalid \"from\" parameter");
            return;
        }

        String toString = getOnlyParameter("to", parameters);
        if (toString == null) {
            printWriter.write("Exactly one \"to\" parameter is required");
            return;
        }
        int to = Integer.parseInt(toString);
        if (to < 0) {
            printWriter.write("Invalid \"to\" parameter");
            return;
        }

        if ("master".equals(fanout)) {
            drainMasterPartition(from, to, printWriter);
        } else {
            drainDataCenterPartition(fanout, from, to, printWriter);
        }
    }

    private void drainMasterPartition(int from, int to, PrintWriter printWriter) {
        if (from < _masterFanoutPartitions) {
            printWriter.write("Cannot drain partition currently in use");
            return;
        }
        if (to >= _masterFanoutPartitions) {
            printWriter.write("Cannot drain to partition not in use");
            return;
        }
        printWriter.write(String.format("Draining master partition %d to partition %d...\n", from, to));
        _eventStore.move(ChannelNames.getMasterFanoutChannel(from), ChannelNames.getMasterFanoutChannel(to));
        printWriter.write("Done!\n");
    }

    private void drainDataCenterPartition(String dataCenter, int from, int to, PrintWriter printWriter) {
        Map<String, DataCenter> availableDataCenters = _dataCenters.getAll().stream()
                .filter(dc -> !_dataCenters.getSelf().equals(dc))
                .collect(Collectors.toMap(DataCenter::getName, dc -> dc));

        DataCenter outboundDataCenter = availableDataCenters.get(dataCenter);
        if (outboundDataCenter == null) {
            printWriter.write("Invalid data center, must be one of " + Joiner.on(",").join(availableDataCenters.keySet()));
            return;
        }
        
        if (from < _dataCenterFanoutPartitions) {
            printWriter.write("Cannot drain partition currently in use");
            return;
        }
        if (to >= _dataCenterFanoutPartitions) {
            printWriter.write("Cannot drain to partition not in use");
            return;
        }
        printWriter.write(String.format("Draining %s partition %d to partition %d...\n", dataCenter, from, to));
        _eventStore.move(ChannelNames.getReplicationFanoutChannel(outboundDataCenter, from), ChannelNames.getReplicationFanoutChannel(outboundDataCenter, to));
        printWriter.write("Done!\n");
    }

    private String getOnlyParameter(String name, ImmutableMultimap<String, String> parameters) {
        Collection<String> values = parameters.get(name);
        if (values.size() != 1) {
            return null;
        }
        return values.iterator().next();
    }
}
