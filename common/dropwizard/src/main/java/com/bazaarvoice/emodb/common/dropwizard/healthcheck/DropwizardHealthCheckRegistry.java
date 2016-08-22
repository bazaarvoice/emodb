package com.bazaarvoice.emodb.common.dropwizard.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import io.dropwizard.setup.Environment;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of {@link HealthCheckRegistry} for Dropwizard {@code Environment} objects.
 */
public class DropwizardHealthCheckRegistry implements HealthCheckRegistry {
    private final Environment _environment;

    @Inject
    public DropwizardHealthCheckRegistry(Environment environment) {
        _environment = checkNotNull(environment, "environment");
    }

    @Override
    public void addHealthCheck(String healthCheckName, HealthCheck healthCheck) {
        _environment.healthChecks().register(healthCheckName, healthCheck);
    }

    @Override
    public Map<String, HealthCheck.Result> runHealthChecks() {
        return _environment.healthChecks().runHealthChecks();
    }
}
