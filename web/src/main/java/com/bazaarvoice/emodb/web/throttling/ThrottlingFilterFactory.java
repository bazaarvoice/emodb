package com.bazaarvoice.emodb.web.throttling;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.sun.jersey.api.model.AbstractMethod;
import com.sun.jersey.spi.container.ResourceFilter;
import com.sun.jersey.spi.container.ResourceFilterFactory;

import javax.annotation.Nullable;
import java.util.List;

/**
 * This factory checks for @ThrottleConcurrentRequests attribute on methods,
 * and instantiates a ResourceFilter for those methods.
 */
public class ThrottlingFilterFactory implements ResourceFilterFactory {

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
    public List<ResourceFilter> create(AbstractMethod abstractMethod) {
        List<ResourceFilter> resourceFilters = Lists.newArrayList();
        if (abstractMethod.isAnnotationPresent(ThrottleConcurrentRequests.class)) {
            int maxRequests = abstractMethod.getAnnotation(ThrottleConcurrentRequests.class).maxRequests();
            InstanceConcurrentRequestRegulatorSupplier regulatorSupplier =
                    new InstanceConcurrentRequestRegulatorSupplier(
                            new DefaultConcurrentRequestRegulator(SEMAPHORE_PROPERTY, maxRequests, _meter));
            resourceFilters.add(new ConcurrentRequestsThrottlingFilter(regulatorSupplier));
        }
        return resourceFilters;
    }
}
