package com.bazaarvoice.emodb.web.throttling;

import com.bazaarvoice.emodb.common.zookeeper.store.ChangeType;
import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

/**
 * This class serves two purposes:
 * <ol>
 *     <li>It manages the set of active throttles and distributes them system wide within the local data center.</li>
 *     <li>It satisfies the {@link DataStoreUpdateThrottler} interface so it can be injected into the API to throttle
 *         calls in accordance with the currently active throttles.</li>
 * </ol>
 *
 * The set of throttles themselves are distributed to the local data center via ZooKeeper.  A {@link MapStore} updates
 * the active throttles in ZooKeeper, and a listener updates the local throttles to match the ZooKeeper state.  For this
 * reason there is a slight propagation delay between updating the rate limit locally and it being enforced throughout
 * the data center, including on the local instance.
 */
public class DataStoreUpdateThrottleManager implements DataStoreUpdateThrottler {

    // Reserved string used for instance-wide rate limiting in ZooKeeper
    private static final String INSTANCE_RATE_LIMIT_ZK_KEY = "_";
    /**
     * A more natural value to indicate the instance-wide rate limit for this class' API would be a wildcard, "*",
     * but this value could inter-operate in unexpected ways as a path wildcard.  Therefore use "*" for the API
     * but use "_" when persisted in Zookeeper.
     */
    public static final String INSTANCE_RATE_LIMIT_KEY = "*";

    private final Logger _log = LoggerFactory.getLogger(DataStoreUpdateThrottleManager.class);

    private final Clock _clock;
    private final MapStore<DataStoreUpdateThrottle> _currentRateLimits;
    private volatile ExpiringRateLimiter _instanceRateLimit = null;
    private final ConcurrentMap<String, ExpiringRateLimiter> _rateLimitByApiKey = Maps.newConcurrentMap();
    private final Histogram _throttleWaitTimeMs;

    @Inject
    public DataStoreUpdateThrottleManager(@DataStoreUpdateThrottleMapStore MapStore<DataStoreUpdateThrottle> currentRateLimits,
                                          Clock clock, MetricRegistry metricRegistry) {
        _currentRateLimits = currentRateLimits;
        _clock = clock;
        _currentRateLimits.addListener((key, changeType) -> onRateLimitChanged(fromZKPath(key), changeType));

        _throttleWaitTimeMs = metricRegistry.histogram(MetricRegistry.name("bv.emodb.web", "Throttle", "throttled-sor-update-ms"));
    }

    public void updateAPIKeyRateLimit(String id, DataStoreUpdateThrottle throttle) {
        checkNotNull(id, "id");
        try {
            _currentRateLimits.set(toZKPath(id), throttle);
        } catch (Exception e) {
            _log.warn("Failed to update rate limit for {}", INSTANCE_RATE_LIMIT_KEY.equals(id) ? "instance" : id, e);
            throw Throwables.propagate(e);
        }
    }

    public void clearAPIKeyRateLimit(String id) {
        try {
            _currentRateLimits.remove(toZKPath(id));
        } catch (Exception e) {
            _log.warn("Failed to clear rate limit for {}", INSTANCE_RATE_LIMIT_KEY.equals(id) ? "instance" : id, e);
            throw Throwables.propagate(e);
        }
    }

    public DataStoreUpdateThrottle getThrottle(String id) {
        return _currentRateLimits.get(toZKPath(id));
    }

    public Map<String, DataStoreUpdateThrottle> getAllThrottles() {
        return _currentRateLimits.getAll().entrySet().stream()
                .filter(e -> _clock.instant().isBefore(e.getValue().getExpirationTime()))
                .collect(Collectors.toMap(e -> fromZKPath(e.getKey()), Map.Entry::getValue));
    }

    @Override
    public void beforeUpdate(String id) {
        checkNotNull(id, "Rate limiting of SOR updates should only be applied to API requests with an API key");

        double microsWaited = 0;
        double maybeMicrosWaited;

        // First, apply instance rate limit
        ExpiringRateLimiter rateLimiter = _instanceRateLimit;
        if (rateLimiter != null) {
            if ((maybeMicrosWaited = rateLimiter.rateLimit()) != -1) {
                microsWaited = maybeMicrosWaited;
            } else {
                synchronized (this) {
                    if (_instanceRateLimit != null && !_instanceRateLimit.isActive()) {
                        // There is a slight race condition here if an admin re-creates the throttle at the exact moment
                        // this throttle is noted to have expired.  However, this is extremely unlikely so we favor
                        // clearing out expired throttles rather than keeping them indefinitely to avoid race conditions.
                        _instanceRateLimit = null;
                        clearAPIKeyRateLimit(INSTANCE_RATE_LIMIT_KEY);
                    }
                }
            }
        }

        // Next apply API Key rate limit
        rateLimiter = _rateLimitByApiKey.get(id);
        if (rateLimiter != null) {
            if ((maybeMicrosWaited = rateLimiter.rateLimit()) != -1) {
                microsWaited += maybeMicrosWaited;
            } else {
                // Same as with the instance throttle we accept an edge race condition so we can clear expired throttles.
                _rateLimitByApiKey.remove(id, rateLimiter);
                clearAPIKeyRateLimit(id);
            }
        }

        // Consider any wait of longer than 1 millisecond to be throttled
        if (microsWaited >= 1000) {
            _throttleWaitTimeMs.update((long) (microsWaited * 1000));
        }
    }

    private void onRateLimitChanged(String key, ChangeType changeType) {
        if (INSTANCE_RATE_LIMIT_KEY.equals(key)) {
            onInstanceRateLimitChanged(changeType == ChangeType.REMOVE);
        } else {
            onAPIKeyRateLimitChanged(key, changeType == ChangeType.REMOVE);
        }
    }

    synchronized private void onInstanceRateLimitChanged(boolean removed) {
        final DataStoreUpdateThrottle rateLimit = removed ? null : _currentRateLimits.get(INSTANCE_RATE_LIMIT_ZK_KEY);

        if (removed || rateLimit == null) {
            _instanceRateLimit = null;
        } else if (_instanceRateLimit == null) {
            _instanceRateLimit = new ExpiringRateLimiter(createRateLimiter(rateLimit.getRateLimit()), rateLimit.getExpirationTime());
        } else {
            _instanceRateLimit = _instanceRateLimit.updated(rateLimit.getRateLimit(), rateLimit.getExpirationTime());
        }
    }

    private void onAPIKeyRateLimitChanged(String id, boolean removed) {
        final DataStoreUpdateThrottle rateLimit = removed ? null : _currentRateLimits.get(id);

        if (removed || rateLimit == null) {
            _rateLimitByApiKey.remove(id);
        } else {
            _rateLimitByApiKey.compute(id, (s, rateLimiter) -> {
                if (rateLimiter == null) {
                    return new ExpiringRateLimiter(createRateLimiter(rateLimit.getRateLimit()), rateLimit.getExpirationTime());
                } else {
                    return rateLimiter.updated(rateLimit.getRateLimit(), rateLimit.getExpirationTime());
                }
            });
        }
    }

    private String toZKPath(String apiKey) {
        requireNonNull(apiKey, "API key cannot be null");
        checkArgument(!INSTANCE_RATE_LIMIT_ZK_KEY.equals(apiKey), "Invalid use of reserved API key");
        return INSTANCE_RATE_LIMIT_KEY.equals(apiKey) ? INSTANCE_RATE_LIMIT_ZK_KEY : apiKey;
    }

    private String fromZKPath(String path) {
        return INSTANCE_RATE_LIMIT_ZK_KEY.equals(path) ? INSTANCE_RATE_LIMIT_KEY : path;
    }

    private final class ExpiringRateLimiter {
        final RateLimiter _rateLimiter;
        final Instant _expirationTime;

        ExpiringRateLimiter(RateLimiter rateLimiter, Instant expirationTime) {
            _rateLimiter = rateLimiter;
            _expirationTime = expirationTime;
        }

        /**
         * Applies the rate limiter if it has not expired.
         * @return The number of microseconds spent waiting due to rate limiting, or -1 if the rate limit was inactive.
         */
        double rateLimit() {
            if (isActive()) {
                return _rateLimiter.acquire();
            } else {
                return -1;
            }
        }

        boolean isActive() {
            return _clock.instant().isBefore(_expirationTime);
        }

        ExpiringRateLimiter updated(double newRate, Instant expirationTime) {
            // If only the expiration time is changing don't adjust the rate limiter
            if (_rateLimiter.getRate() == newRate) {
                return new ExpiringRateLimiter(_rateLimiter, expirationTime);
            } else {
                return new ExpiringRateLimiter(createRateLimiter(newRate), expirationTime);
            }
        }

        DataStoreUpdateThrottle getMetadata() {
            return new DataStoreUpdateThrottle(_rateLimiter.getRate(), _expirationTime);
        }
    }

    /**
     * Simple method to create a rate limiter.  It is exposed as a protected method to allow unit tests to override
     * to enable introspecting rate limit calls.
     */
    @VisibleForTesting
    protected RateLimiter createRateLimiter(double rate) {
        return RateLimiter.create(rate);
    }
}
