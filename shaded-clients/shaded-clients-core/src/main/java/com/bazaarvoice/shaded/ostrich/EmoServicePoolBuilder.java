package com.bazaarvoice.shaded.ostrich;

import com.codahale.metrics.MetricRegistry;

/**
 * Helper for creating a service pool builder with the unused MetricRegistry preset.
 * @param <S>
 */
public class EmoServicePoolBuilder<S> {
    public static <S> com.bazaarvoice.ostrich.pool.ServicePoolBuilder<S> create(Class<S> serviceClass) {
        return com.bazaarvoice.ostrich.pool.ServicePoolBuilder.create(serviceClass)
                .withMetricRegistry(new MetricRegistry());
    }
}
