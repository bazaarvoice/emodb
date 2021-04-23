package com.bazaarvoice.emodb.web.throttling;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import javax.annotation.Nullable;
import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.FeatureContext;

/**
 * This factory checks for @ThrottleConcurrentRequests attribute on methods,
 * and instantiates a ResourceFilter for those methods.
 */
public class ThrottlingFilterFactory implements DynamicFeature {

    private final static String SEMAPHORE_PROPERTY = ThrottlingFilterFactory.class.getName() + ".semaphore";

    private final Meter _meter;

    public ThrottlingFilterFactory(@Nullable MetricRegistry metricRegistry) {
        if (metricRegistry != null) {
            _meter = metricRegistry.meter(MetricRegistry.name("bv.emodb.web", "Throttle", "throttled-requests"));
        } else {
            _meter = null;
        }
    }

    @Override
    public void configure(ResourceInfo resourceInfo, FeatureContext context) {
        if (resourceInfo.getResourceMethod().isAnnotationPresent(ThrottleConcurrentRequests.class)) {
            int maxRequests = resourceInfo.getResourceMethod().getAnnotation(ThrottleConcurrentRequests.class).maxRequests();
            InstanceConcurrentRequestRegulatorSupplier regulatorSupplier =
                    new InstanceConcurrentRequestRegulatorSupplier(
                            new DefaultConcurrentRequestRegulator(SEMAPHORE_PROPERTY, maxRequests, _meter));
            context.register(new ConcurrentRequestsThrottlingFilter(regulatorSupplier));
        }
    }
}
