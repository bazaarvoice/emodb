package com.bazaarvoice.emodb.auth.jersey;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.sun.jersey.spi.container.ResourceFilterFactory;

import javax.annotation.Nullable;
import java.util.List;

/**
 * This class holds objects which can be used to configure Jersey to allow for authentication and authorization.
 * @see JerseyAuthConfigurationBuilder
 */
public class JerseyAuthConfiguration {

    private final List<Object> _providers;
    private final List<ResourceFilterFactory> _resourceFilterFactories;

    public JerseyAuthConfiguration(List<Object> providers, List<ResourceFilterFactory> resourceFilterFactories) {
        _providers = providers;
        _resourceFilterFactories = resourceFilterFactories;
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

    public List<ResourceFilterFactory> getResourceFilterFactories() {
        return _resourceFilterFactories;
    }
}
