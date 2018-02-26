package com.bazaarvoice.emodb.common.dropwizard.leader;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.zookeeper.leader.PartitionedLeaderService;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Shows the current status of leadership processes managed by {@link LeaderService}.  Allows terminating
 * individual leadership processes, but such that they can be restarted only by restarting the entire server.
 */
public class LeaderServiceTask extends Task {
    private static final Logger _log = LoggerFactory.getLogger(LeaderServiceTask.class);

    private final ConcurrentMap<String, LeaderService> _selectorMap = Maps.newConcurrentMap();

    @Inject
    public LeaderServiceTask(TaskRegistry tasks) {
        super("leader");
        tasks.addTask(this);
    }

    public void register(final String name, final LeaderService leaderService) {
        _selectorMap.put(name, leaderService);

        // Unregister automatically to avoid memory leaks.
        leaderService.addListener(new AbstractServiceListener() {
            @Override
            public void terminated(Service.State from) {
                unregister(name, leaderService);
            }

            @Override
            public void failed(Service.State from, Throwable failure) {
                unregister(name, leaderService);
            }
        }, MoreExecutors.sameThreadExecutor());
    }

    public void register(final String name, final PartitionedLeaderService partitionedLeaderService) {
        int partition = 0;
        for (LeaderService leaderService : partitionedLeaderService.getPartitionLeaderServices()) {
            register(String.format("%s-%d", name, partition++), leaderService);
        }
    }

    public void unregister(String name, LeaderService leaderService) {
        _selectorMap.remove(name, leaderService);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter out) throws Exception {
        // The 'release' argument tells a server to give up leadership and let a new leader be elected, possibly
        // re-electing the current server.  This is useful for rebalancing leader-controlled activities.
        for (String name : parameters.get("release")) {
            LeaderService leaderService = _selectorMap.get(name);
            if (leaderService == null) {
                out.printf("Unknown leader process: %s%n", name);
                continue;
            }

            Service actualService = leaderService.getCurrentDelegateService().orNull();
            if (actualService == null || !actualService.isRunning()) {
                out.printf("Process is not currently elected leader: %s%n", name);
                continue;
            }

            _log.warn("Temporarily releasing leadership for process: {}", name);
            out.printf("Temporarily releasing leadership for process: %s, cluster will elect a new leader.%n", name);
            actualService.stopAndWait();
        }

        // The 'terminate' argument tells a server to give up leadership permanently (or until the server restarts).
        for (String name : parameters.get("terminate")) {
            LeaderService leaderService = _selectorMap.get(name);
            if (leaderService == null) {
                out.printf("Unknown leader process: %s%n", name);
                continue;
            }

            _log.warn("Terminating leader process for: {}", name);
            out.printf("Terminating leader process for: %s. Restart the server to restart the leader process.%n", name);
            leaderService.stopAndWait();
        }

        // Print current status.
        for (Map.Entry<String, LeaderService> entry : new TreeMap<>(_selectorMap).entrySet()) {
            String name = entry.getKey();
            LeaderService leaderService = entry.getValue();

            out.printf("%s: %s (leader=%s)%n", name,
                    describeState(leaderService.state(), leaderService.hasLeadership()),
                    getLeaderId(leaderService));
        }
    }

    private String describeState(Service.State state, boolean hasLeadership) {
        if (state == Service.State.RUNNING && !hasLeadership) {
            return "waiting to win leadership election";
        } else {
            return state.name();
        }
    }

    private String getLeaderId(LeaderService leaderService) {
        try {
            return leaderService.getLeader().getId();
        } catch (Exception e) {
            return "<unknown>";
        }
    }
}
