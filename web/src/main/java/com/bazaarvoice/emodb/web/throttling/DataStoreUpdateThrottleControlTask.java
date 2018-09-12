package com.bazaarvoice.emodb.web.throttling;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkDurationSerializer;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Dropwizard task to throttle DataStore updates via the REST API by API key and/or instance wide.  When a rate limit
 * is active for the instance then all API updates are rate limited.  No guarantees to fairness are made for which API
 * keys get rate limited.  When an API key is rate limited and there is an instance rate limit then both are applied.
 * For example, if the instance rate limit is set to 500 wps and an API key is rate limited to 100 wps then a write by
 * the API key may be rate limited even if it is writing at below 100 wps if in aggregate the instance is receiving over
 * 500 wps.
 *
 * Note that the actual number of updates are what is rate limited by this task, not the number of update API calls.
 * For example, 500 individual calls to update 500 documents is throttled at the same rate as if a single streaming call
 * were made which updates the same 500 documents.   Additionally, only updates made via the API are rate limited.
 * Updates made internally by EmoDB, such as during a purge operation, or as a consequence of another action, such as
 * by creating a table, are not rate limited by this task.
 *
 * Whether per API key or instance wide all rate limits are expressed and enforced at the instance level.  There is no way
 * to specify a global rate limit across the entire cluster.  If an API key is rate limited to 200 wps and there are 4
 * instances in the cluster then, assuming the API key is maximally utilizing all four instances, it can achieve an
 * aggregate 800 wps across the cluster.  If a fifth server is added to the cluster its maximum throughput rises to
 * 1,000 wps.
 *
 * Throttles are applied only within the local data center.  For example, rate limiting an API key in us-east-1 will
 * have no effect on throttling writes in eu-west-1.  This task would have to be called on a server in each data center
 * to rate limit the API key in both.  However, any updates made using this task are automatically propagated to every
 * server in the local data center.
 *
 * All throttles must expire after a specific duration.  If none is provided the default is 24 hours.  To remove all
 * throttling for an API key set the rate limit to 0.  Note that it is not possible to completely block update access
 * using this task, but it can be set remarkably low, such as to 0.1 (1 update every 10 seconds).
 *
 * API keys are expressed using their IDs, not the private keys.  Using this task with a key and not the key's ID will
 * not return an error but it will also not rate limit any API keys.  To throttle updates instance wide across all API keys
 * use the wildcard API key, "*".
 *
 * Usage:
 * <pre>
 *     # Set a throttle for a single API key, with the default and with an explicit expiration
 *     curl -s -XPOST 'localhost:8081/tasks/sor-api-update-throttle?id=ID01&limit=100'
 *     curl -s -XPOST 'localhost:8081/tasks/sor-api-update-throttle?id=ID02&limit=100&duration=PT1H'
 *
 *     # Set an instance wide throttle
 *     curl -s -XPOST 'localhost:8081/tasks/sor-api-update-throttle?id=*&limit=500&duration=PT30M'
 *
 *     # Remove a throttle
 *      curl -s -XPOST 'localhost:8081/tasks/sor-api-update-throttle?id=ID02&limit=0'
 *
 *     # List all throttled endpoints
 *      curl -s -XPOST 'localhost:8081/tasks/sor-api-update-throttle'
 *
 *     # Clear all throttled endpoints
 *     curl -s -XPOST 'localhost:8081/tasks/sor-api-update-throttle?clear'
 * </pre>
 */
public class DataStoreUpdateThrottleControlTask extends Task {
    private final DataStoreUpdateThrottleManager _throttleManager;

    @Inject
    public DataStoreUpdateThrottleControlTask(TaskRegistry taskRegistry,
                                              DataStoreUpdateThrottleManager throttleManager) {
        super("sor-api-update-throttle");
        _throttleManager = requireNonNull(throttleManager, "throttleManager");
        taskRegistry.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter out) throws Exception {
        // Default duration
        Duration expiryDuration = Duration.ofHours(24);
        Collection<String> values = parameters.get("duration");
        if (values.size() == 1) {
            expiryDuration = new ZkDurationSerializer().fromString(values.iterator().next());
        } else if (values.size() > 1) {
            throw new IllegalArgumentException("At most one duration parameter is permitted");
        }

        double limit = 0;
        values = parameters.get("limit");
        if (values.size() == 1) {
            limit = Double.parseDouble(values.iterator().next());
        } else if (values.size() > 1) {
            throw new IllegalArgumentException("At most one limit parameter is permitted");
        }

        boolean throttlesChanged = false;

        if (parameters.keySet().contains("clear")) {
            for (String apiKey : _throttleManager.getAllThrottles().keySet()) {
                _throttleManager.clearAPIKeyRateLimit(apiKey);
            }
            out.println("All throttles cleared");
            throttlesChanged = true;
        }

        if (!parameters.get("id").isEmpty()) {
            DataStoreUpdateThrottle throttle = null;
            if (limit > 0) {
                throttle = new DataStoreUpdateThrottle(limit, Instant.now().plus(expiryDuration));
                out.printf("Applying throttled rate limit %f expires at %s:\n", limit, throttle.getExpirationTime());
            } else {
                out.printf("Removing throttles:\n");
            }

            for (String id : parameters.get("id")) {
                if (throttle != null) {
                    _throttleManager.updateAPIKeyRateLimit(id, throttle);
                } else {
                    _throttleManager.clearAPIKeyRateLimit(id);
                }
                out.printf("- Applied to %s\n", id);
            }

            throttlesChanged = true;
        }

        if (throttlesChanged) {
            out.println();
            out.println("NOTE: Due to propagation delays the effects of these changes may not be immediately reflected " +
                    "in the following list of active throttles.");
            out.println();

            // Wait 500ms to give propagation a better chance at having been applied when dumping the list of active throttles
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignore) {
                // ignore
            }
        }

        out.println("Active throttles:\n");
        for (Map.Entry<String, DataStoreUpdateThrottle> entry : _throttleManager.getAllThrottles().entrySet()) {
            String apiKey = entry.getKey();
            DataStoreUpdateThrottle throttle = entry.getValue();

            out.printf("- %s (limit %f expires at %s)\n", apiKey, throttle.getRateLimit(), throttle.getExpirationTime());
        }

        out.println();
    }
}
