package com.bazaarvoice.emodb.common.dropwizard.discovery;

import com.bazaarvoice.ostrich.ServiceRegistry;

import javax.ws.rs.Path;

/**
 * Registers a Jersey resource with Dropwizard and ZooKeeper.
 */
public interface ResourceRegistry {

    /**
     * Adds a Jersey resource annotated with the {@link Path} annotation and registers it using the specified namespace
     * and service name within a SOA {@link ServiceRegistry}.
     */
    void addResource(String namespace, String service, Object resource);
}
