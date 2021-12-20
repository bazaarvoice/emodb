package com.bazaarvoice.emodb.web.throttling;

import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * This class manages all system-wide ad-hoc throttles.  Clients can use it to create, remove, and query
 * ad-hoc throttles for specific requests.  Note that it is not responsible for determining how the throttles are
 * applied, only for maintaining the metadata of what throttles are in place.
 */
public class AdHocThrottleManager {
    private final Logger _log = LoggerFactory.getLogger(AdHocThrottleManager.class);

    private final MapStore<AdHocThrottle> _throttleMap;

    @Inject
    public AdHocThrottleManager(@AdHocThrottleMapStore MapStore<AdHocThrottle> throttleMap) {
        _throttleMap = requireNonNull(throttleMap, "throttleMap");
    }

    /**
     * Adds a throttle for an HTTP method and path.
     */
    public void addThrottle(AdHocThrottleEndpoint endpoint, AdHocThrottle throttle) {
        requireNonNull(throttle, "throttle");

        String key = endpoint.toString();
        try {
            // If the throttle is unlimited make sure it is not set, since the absence of a value indicates unlimited
            if (throttle.isUnlimited()) {
                _throttleMap.remove(key);
            }
            _throttleMap.set(key, throttle);
        } catch (Exception e) {
            Throwables.propagateIfPossible(e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Removes the throttle for an HTTP method and path.  This effectively allows umlimited concurrency.
     */
    public void removeThrottle(AdHocThrottleEndpoint endpoint) {
        try {
            _throttleMap.remove(endpoint.toString());
        } catch (Exception e) {
            _log.warn("Failed to remove throttle for {} {}", endpoint.getMethod(), endpoint.getPath(), e);
        }
    }

    /**
     * Returns the throttle in effect for the given HTTP method and path.  This method is always guaranteed to return
     * a non-null throttle that is not expired.  If no throttle has been configured or if the configured throttle has
     * expired then it will return an unlimited throttle with an effectively infinite expiration date.
     */
    public AdHocThrottle getThrottle(AdHocThrottleEndpoint endpoint) {
        String key = endpoint.toString();
        AdHocThrottle throttle = _throttleMap.get(key);
        if (throttle == null) {
            // No throttle set, allow unlimited
            throttle = AdHocThrottle.unlimitedInstance();
        } else if (throttle.getExpiration().isBefore(Instant.now())) {
            // Throttle is expired; remove it and allow unlimited.  There is a slight chance for a race condition
            // here but since throttles are rarely put in place this is extremely unlikely.  To help avoid this
            // wait 24 hours before removing.
            if (throttle.getExpiration().isBefore(Instant.now().minus(Duration.ofDays(1)))) {
                try {
                    _throttleMap.remove(key);
                } catch (Exception e) {
                    _log.warn("Failed to remove expired throttle for {} {}", endpoint.getMethod(), endpoint.getPath(), e);
                }
            }
            throttle = AdHocThrottle.unlimitedInstance();
        }
        return throttle;
    }

    public Map<AdHocThrottleEndpoint, AdHocThrottle> getAllThrottles() {
        Map<AdHocThrottleEndpoint, AdHocThrottle> map = Maps.newHashMap();
        for (Map.Entry<String, AdHocThrottle> entry : _throttleMap.getAll().entrySet()) {
            AdHocThrottle throttle = entry.getValue();
            // Filter out throttles which are not in effect
            if (!throttle.isUnlimited() && throttle.getExpiration().isAfter(Instant.now())) {
                map.put(AdHocThrottleEndpoint.fromString(entry.getKey()), throttle);
            }
        }
        return map;
    }
}
