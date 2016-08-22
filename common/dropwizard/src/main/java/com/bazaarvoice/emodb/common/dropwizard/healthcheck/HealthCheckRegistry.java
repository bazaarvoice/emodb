package com.bazaarvoice.emodb.common.dropwizard.healthcheck;

import com.codahale.metrics.health.HealthCheck;

import java.util.Map;

/**
 * Registry of Dropwizard {@link HealthCheck} objects.
 */
public interface HealthCheckRegistry {
    void addHealthCheck(String healthCheckName, HealthCheck healthCheck);

    Map<String, HealthCheck.Result> runHealthChecks();
}
