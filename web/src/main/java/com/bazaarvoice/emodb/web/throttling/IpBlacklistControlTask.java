package com.bazaarvoice.emodb.web.throttling;

import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.zookeeper.Sync;
import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkDurationSerializer;
import com.fasterxml.jackson.databind.util.ISO8601Utils;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

/**
 * Dropwizard task to update the black list of offending IpAddress
 * <p/>
 * Usage:
 * <pre>
 *     # Add a list of IPs with the expiry duration (optional, by default it's 24 hours)
 *     curl -s -XPOST 'localhost:8081/tasks/blacklist?add=127.0.0.1,122.0.0.1
 *     curl -s -XPOST 'localhost:8081/tasks/blacklist?add=127.0.0.1,122.0.0.1&duration=PT30M
 *
 *     # Remove an IP
 *     curl -s -XPOST 'localhost:8081/tasks/blacklist?remove=127.0.0.1,122.0.9.1'
 *
 *     # Clear the black list
 *     curl -s -XPOST 'localhost:8081/tasks/blacklist?clear'
 * </pre>
 */
public class IpBlacklistControlTask extends Task {
    private static final Logger _log = LoggerFactory.getLogger(IpBlacklistControlTask.class);

    enum Action {add, remove, clear, duration}

    private final MapStore<Long> _mapStore;
    private final CuratorFramework _curator;

    @Inject
    public IpBlacklistControlTask(TaskRegistry taskRegistry,
                                  @Global CuratorFramework curator,
                                  @BlackListIpValueStore MapStore<Long> mapStore) {
        super("blacklist");
        _curator = requireNonNull(curator, "curator");
        _mapStore = requireNonNull(mapStore, "mapStore");
        taskRegistry.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter out) throws Exception {
        boolean changed = false;
        // Default duration
        Duration expiryDuration = Duration.ofHours(24);

        // Find if user specified a duration override
        for (String durationString : parameters.get(Action.duration.toString())) {
            expiryDuration = new ZkDurationSerializer().fromString(durationString);
        }

        for (Map.Entry<String, String> entry : parameters.entries()) {
            Action action;
            try {
                action = Action.valueOf(entry.getKey());
            } catch (IllegalArgumentException e) {
                out.printf("Unrecognized action parameter: %s%n", entry.getKey());
                continue;
            }
            if (action == Action.add || action == Action.remove) {
                String commaDelimitedIps = entry.getValue();
                if (Strings.isNullOrEmpty(commaDelimitedIps)) {
                    out.printf("No IP address specified%n");
                    return;
                }
                String[] blackList = commaDelimitedIps.split(",");
                for (String ipAddress : blackList) {
                    updateAction(action, ipAddress, expiryDuration);
                    changed = true;
                }
            }
            if (action == Action.clear) {
                // clear the black list
                for (Map.Entry<String, Long> nodeEntry : _mapStore.getAll().entrySet()) {
                    updateAction(action, nodeEntry.getKey(), expiryDuration);
                }
                changed = true;
            }
        }

        if (changed) {
            // Wait briefly for round trip through ZooKeeper.
            if (!Sync.synchronousSync(_curator, Duration.ofSeconds(1))) {
                out.println("WARNING: Timed out after one second waiting for updates to round trip through ZooKeeper.");
            }
            Thread.sleep(50);  // Wait a little bit longer for NodeDiscovery listeners to fire.
        }

        out.printf("Black Listed IPs:%n");
        for (Map.Entry<String, Long> entry : new TreeMap<>(_mapStore.getAll()).entrySet()) {
            out.printf("%s (expires %s)%n", entry.getKey(), ISO8601Utils.format(new Date(entry.getValue()), false));
        }
    }

    private void updateAction(Action action, String ip, Duration duration)
            throws Exception {
        if (action == Action.add) {
            _mapStore.set(ip, Instant.now().plus(duration).toEpochMilli());
            _log.info("ip added to the black list: {}", ip);
        }
        if (action == Action.remove || action == Action.clear) {
            _mapStore.remove(ip);
            _log.info("ip removed from the black list: {}", ip);
        }
    }
}
