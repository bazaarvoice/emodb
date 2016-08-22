package com.bazaarvoice.emodb.web.partition;

import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceFactory;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps a SOA {@link ServiceFactory} and returns SOA clients for remote end points and the in-JVM service
 * implementation for the end point corresponding to the local server.
 */
public class PartitionAwareServiceFactory<S> implements MultiThreadedServiceFactory<S> {
    private final MultiThreadedServiceFactory<S> _delegate;
    private final S _local;
    private final String _localId;
    private final HealthCheckRegistry _healthCheckRegistry;

    public PartitionAwareServiceFactory(MultiThreadedServiceFactory<S> delegate, S local, HostAndPort self, HealthCheckRegistry healthCheckRegistry) {
        _delegate = checkNotNull(delegate, "delegate");
        _local = checkNotNull(local, "local");
        _localId = self.toString();
        _healthCheckRegistry = healthCheckRegistry;
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
        return _delegate.create(endPoint);
    }

    @Override
    public void destroy(ServiceEndPoint endPoint, S service) {
        if (!isSelf(endPoint)) {
            _delegate.destroy(endPoint, service);
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
}
