package com.bazaarvoice.emodb.web.throttling;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;
import javax.ws.rs.container.ContainerRequestContext;

import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link ConcurrentRequestRegulatorSupplier} which supplies regulators which will throttle based
 * on any ad-hoc throttles created by {@link AdHocThrottleControlTask}.
 */
public class AdHocConcurrentRequestRegulatorSupplier implements ConcurrentRequestRegulatorSupplier {

    private final static String SEMAPHORE_PROPERTY = AdHocConcurrentRequestRegulatorSupplier.class.getName() + ".semaphore";

    private final AdHocThrottleManager _throttleStore;
    private final ConcurrentMap<AdHocThrottleEndpoint, CachedRegulator> _regulatorCache = Maps.newConcurrentMap();
    private final Meter _meter;

    public AdHocConcurrentRequestRegulatorSupplier(AdHocThrottleManager throttleStore, MetricRegistry metricRegistry) {
        _throttleStore = requireNonNull(throttleStore, "throttleStore");
        _meter = metricRegistry.meter(MetricRegistry.name("bv.emodb.web", "Throttle", "adhoc-throttled-requests"));
    }

    @Override
    public ConcurrentRequestRegulator forRequest(ContainerRequestContext request) {
        return forRequest(request.getMethod(), request.getUriInfo().getPath());
    }

    public ConcurrentRequestRegulator forRequest(String method, String path) {
        // Get the currently cached regulator for this request.
        AdHocThrottleEndpoint endpoint = new AdHocThrottleEndpoint(method, path);
        CachedRegulator cachedRegulator = _regulatorCache.get(endpoint);

        AdHocThrottle throttle = _throttleStore.getThrottle(endpoint);
        if (throttle.isUnlimited()) {
            // Unlimited is the default and doesn't get cached.  So if there is a cached value then remove it.
            if (cachedRegulator != null) {
                _regulatorCache.remove(endpoint);
            }
            return UnthrottledConcurrentRequestRegulator.instance();
        }

        // If the cached regulator doesn't match the throttle then update it
        if (cachedRegulator == null || !cachedRegulator.throttle.equals(throttle)) {
            // The following code allow for multiple threads to get the same cached regulator concurrently without blocking.
            ConcurrentRequestRegulator regulator =
                    new DefaultConcurrentRequestRegulator(SEMAPHORE_PROPERTY, throttle.getLimit(), _meter);
            CachedRegulator updatedValue = new CachedRegulator(throttle, regulator);

            while (cachedRegulator == null || !cachedRegulator.throttle.equals(updatedValue.throttle)) {
                if (cachedRegulator == null) {
                    cachedRegulator = Optional.ofNullable(_regulatorCache.putIfAbsent(endpoint, updatedValue)).orElse(updatedValue);
                } else if (_regulatorCache.replace(endpoint, cachedRegulator, updatedValue)) {
                    cachedRegulator = updatedValue;
                } else {
                    cachedRegulator = _regulatorCache.get(endpoint);
                }
            }
        }

        return cachedRegulator.regulator;
    }

    /**
     * Internal helper object which contains an AdHocThrottle and a regulator whose functional behavior
     * matches the throttles parameters.
     */
    private static class CachedRegulator {
        AdHocThrottle throttle;
        ConcurrentRequestRegulator regulator;

        private CachedRegulator(AdHocThrottle throttle, ConcurrentRequestRegulator regulator) {
            this.throttle = throttle;
            this.regulator = regulator;
        }
    }
}
