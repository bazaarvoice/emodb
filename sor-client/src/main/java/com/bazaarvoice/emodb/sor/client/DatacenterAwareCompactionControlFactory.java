package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class DatacenterAwareCompactionControlFactory<S> implements MultiThreadedServiceFactory<S> {
    private final MultiThreadedServiceFactory<S> _delegate;
    private final S _local;
    private final String _selfDatacenterName;
    private final HealthCheckRegistry _healthCheckRegistry;

    public DatacenterAwareCompactionControlFactory(MultiThreadedServiceFactory<S> delegate, S local, String selfDatacenterName, HealthCheckRegistry healthCheckRegistry) {
        _delegate = checkNotNull(delegate, "delegate");
        _local = checkNotNull(local, "local");
        _selfDatacenterName = checkNotNull(selfDatacenterName, "selfDatacenterName");
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
        return _selfDatacenterName.equals(endPoint.getId());
    }

    @Override
    public boolean isRetriableException(Exception e) {
        return _delegate.isRetriableException(e);
    }
}