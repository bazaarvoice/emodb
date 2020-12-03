package com.bazaarvoice.emodb.auth.jersey;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;

import javax.annotation.Nullable;
import java.util.List;
import javax.ws.rs.container.DynamicFeature;

/**
 * This class holds objects which can be used to configure Jersey to allow for authentication and authorization.
 * @see JerseyAuthConfigurationBuilder
 */
public class JerseyAuthConfiguration {

    private final List<Object> _providers;
    private final List<DynamicFeature> _dynamicFeatures;

    public JerseyAuthConfiguration(List<Object> providers, List<DynamicFeature> dynamicFeatures) {
        _providers = providers;
        _dynamicFeatures = dynamicFeatures;
    }

    public List<Object> getProviderInstances() {
        return FluentIterable.from(_providers)
                .filter(Predicates.not(Predicates.instanceOf(Class.class)))
                .toList();
    }

    public List<Class<?>> getProviderClasses() {
        return FluentIterable.from(_providers)
                .transform(new Function<Object, Class<?>>() {
                    @Nullable
                    @Override
                    public Class<?> apply(Object o) {
                        return o instanceof Class ? (Class<?>) o : null;
                    }
                })
                .filter(Predicates.notNull())
                .toList();
    }

    public List<DynamicFeature> getDynamicFeatures() {
        return _dynamicFeatures;
    }
}
