package com.bazaarvoice.emodb.web.throttling;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkDurationSerializer;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Dropwizard task to add and update ad-hoc throttling by API endpoint.  A few caveats:
 * <ul>
 *     <li>
 *         The endpoint throttle is applied using an <em>exact</em> endpoint match.  For example throttling
 *         GET requests to /queue/1/polloi won't throttle a GET request to /queue/1/polloi-trigger-queue.
 *     </li>
 *     <li>
 *         Each throttle is applied independently per application server instance.  For example
 *         if GET requests to /queue/1/popular-queue/size were throttled to 1 concurrent request
 *         and there are 4 application servers then there could be up to 4 concurrent size operations
 *         across the entire system.
 *     </li>
 *     <li>
 *         Whenever the limit for an endpoint changes there is a brief window during the switch
 *         where the new limit may be violated.  This is because the new limit only affects new requests
 *         and any requests in-flight at the time are still throttled using the old throttler.
 *     </li>
 * </ul>
 * <p/>
 * Usage:
 * <pre>
 *     # Add or update an endpoint (method and duration are optional, defaults are GET and 24 hours respectively)
 *     curl -s -XPOST 'localhost:8081/tasks/adhoc-throttle?add=/dedupq/1/popular-queue/size&limit=10
 *     curl -s -XPOST 'localhost:8081/tasks/adhoc-throttle?add=/dedupq/1/popular-queue/send&method=POST&limit=5&duration=PT30m'
 *
 *     # Remove an endpoint
 *     curl -s -XPOST 'localhost:8081/tasks/adhoc-throttle?remove=/dedupq/1/popular-queue/size'
 *
 *     # List all throttled endpoints
 *     curl -s -XPOST 'localhost:8081/tasks/adhoc-throttle?list'
 *
 *     # Clear all throttled endpoints
 *     curl -s -XPOST 'localhost:8081/tasks/adhoc-throttle?clear'
 * </pre>
 *
 * Note that setting the limit to 0 effectively blocks all access to the throttled endpoint.
 */
public class AdHocThrottleControlTask extends Task {
    private static final Logger _log = LoggerFactory.getLogger(AdHocThrottleControlTask.class);

    enum Action {add, remove, clear, list}

    private final AdHocThrottleManager _throttleManager;

    @Inject
    public AdHocThrottleControlTask(TaskRegistry taskRegistry,
                                    AdHocThrottleManager throttleManager) {
        super("adhoc-throttle");
        _throttleManager = checkNotNull(throttleManager, "throttleManager");
        taskRegistry.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter out) throws Exception {
        // Default method
        String method = "GET";
        // Default duration
        Duration expiryDuration = Duration.ofHours(24);

        // Get specified parameters related for method and duration, if present
        for (String string : parameters.get("method")) {
            method = string;
        }
        for (String string : parameters.get("duration")) {
            expiryDuration = new ZkDurationSerializer().fromString(string);
        }

        if (parameters.keySet().contains(Action.clear.toString())) {
            for (AdHocThrottleEndpoint endpoint : _throttleManager.getAllThrottles().keySet()) {
                _throttleManager.removeThrottle(endpoint);
            }
            out.println("All throttles cleared");
        }

        if (parameters.keySet().contains(Action.remove.toString())) {
            for (String path: parameters.get(Action.remove.toString())) {
                _throttleManager.removeThrottle(new AdHocThrottleEndpoint(method, path));
                out.printf("Endpoint throttle removed: %s %s\n", method, path);
            }
        }

        if (parameters.keySet().contains(Action.add.toString())) {
            for (String path : parameters.get(Action.add.toString())) {
                // Get the specified limit
                Integer limit = null;
                for (String string : parameters.get("limit")) {
                    limit = Integer.parseInt(string);
                }
                Instant expiryTime = Instant.now().plus(expiryDuration);

                if (limit == null) {
                    out.printf("Limit is required; throttle unchanged for %s %s\n", method, path);
                } else {
                    _throttleManager.addThrottle(new AdHocThrottleEndpoint(method, path), AdHocThrottle.create(limit, expiryTime));
                    out.printf("Endpoint throttled to %d concurrent requests: %s %s", limit, method, path);
                }
            }
        }

        if (parameters.keySet().contains(Action.list.toString())) {
            for (Map.Entry<AdHocThrottleEndpoint, AdHocThrottle> entry : _throttleManager.getAllThrottles().entrySet()) {
                AdHocThrottleEndpoint endpoint = entry.getKey();
                AdHocThrottle throttle = entry.getValue();

                out.printf("%s %s (limit %d expires at %s)\n", endpoint.getMethod(), endpoint.getPath(),
                        throttle.getLimit(), throttle.getExpiration());
            }
        }
    }
}