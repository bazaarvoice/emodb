package com.bazaarvoice.emodb.web.partition;

import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceFactory;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.reflect.Reflection;
import com.sun.jersey.api.client.ClientHandlerException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps a SOA {@link ServiceFactory} and returns SOA clients for remote end points and the in-JVM service
 * implementation for the end point corresponding to the local server.
 */
public class PartitionAwareServiceFactory<S> implements MultiThreadedServiceFactory<S> {
    private final Class<S> _serviceClass;
    private final MultiThreadedServiceFactory<S> _delegate;
    private final S _local;
    private final String _localId;
    private final HealthCheckRegistry _healthCheckRegistry;
    private final Meter _errorMeter;
    private final Map<S, S> _proxiedToDelegateServices = Maps.newIdentityHashMap();

    public PartitionAwareServiceFactory(Class<S> serviceClass, MultiThreadedServiceFactory<S> delegate, S local,
                                        HostAndPort self, HealthCheckRegistry healthCheckRegistry,
                                        MetricRegistry metricRegistry) {
        _serviceClass = checkNotNull(serviceClass, "serviceClass");
        _delegate = checkNotNull(delegate, "delegate");
        _local = checkNotNull(local, "local");
        _localId = self.toString();
        _healthCheckRegistry = healthCheckRegistry;
        _errorMeter = metricRegistry.meter(MetricRegistry.name("bv.emodb.web.partition-forwarding",
                serviceClass.getSimpleName(), "errors"));
    }

    @Override
    public String getServiceName() {
        return _delegate.getServiceName();
    }

    @Override
    public void configure(ServicePoolBuilder<S> servicePoolBuilder) {
        _delegate.configure(servicePoolBuilder);
    }

    @Override
    public S create(ServiceEndPoint endPoint) {
        if (isSelf(endPoint)) {
            return _local;
        }
        return createDelegate(endPoint);
    }

    @Override
    public void destroy(ServiceEndPoint endPoint, S service) {
        if (!isSelf(endPoint)) {
            destoryDelegate(endPoint, service);
        }
    }

    @Override
    public boolean isHealthy(ServiceEndPoint endPoint) {
        if (isSelf(endPoint)) {
            Map<String, HealthCheck.Result> results = _healthCheckRegistry.runHealthChecks();
            return Iterables.all(results.values(), new Predicate<HealthCheck.Result>() {
                @Override
                public boolean apply(HealthCheck.Result result) {
                    return result.isHealthy();
                }
            });
        }
        return _delegate.isHealthy(endPoint);
    }

    private boolean isSelf(ServiceEndPoint endPoint) {
        return _localId.equals(endPoint.getId());
    }

    @Override
    public boolean isRetriableException(Exception e) {
        return _delegate.isRetriableException(e);
    }

    private S createDelegate(ServiceEndPoint endPoint) {
        final S delegateService = _delegate.create(endPoint);

        S proxiedService = Reflection.newProxy(_serviceClass, new AbstractInvocationHandler() {
            @Override
            protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
                try {
                    return method.invoke(delegateService, args);
                } catch (InvocationTargetException e) {
                    // If the target exception is a declared exception then rethrow as-is
                    Throwable targetException = e.getTargetException();
                    for (Class<?> declaredException : method.getExceptionTypes()) {
                        // noinspection unchecked
                        Throwables.propagateIfInstanceOf(targetException, (Class<? extends Throwable>) declaredException);
                    }
                    // If the exception was due to connection issues and not necessarily the target let the caller know.
                    // It's possible the connection timed out due to a problem on the target, but from our perspective
                    // there's no definitive way of knowing.
                    if (targetException instanceof ClientHandlerException) {
                        _errorMeter.mark();
                        throw new PartitionForwardingException("Failed to handle request at endpoint", targetException.getCause());
                    }
                    throw Throwables.propagate(targetException);
                }
            }
        });

        _proxiedToDelegateServices.put(proxiedService, delegateService);
        return proxiedService;
    }

    private void destoryDelegate(ServiceEndPoint endPoint, S proxiedService) {
        S delegateService = _proxiedToDelegateServices.remove(proxiedService);
        if (delegateService != null) {
            _delegate.destroy(endPoint, delegateService);
        }
    }
}
