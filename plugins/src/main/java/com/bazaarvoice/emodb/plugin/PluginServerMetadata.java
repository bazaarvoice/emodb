package com.bazaarvoice.emodb.plugin;

import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.google.common.net.HostAndPort;
import org.apache.curator.framework.CuratorFramework;

/**
 * Metadata that is passed to all plugins on initialization.
 */
public interface PluginServerMetadata {
    /**
     * Returns the service mode for the EmoDB server.
     */
    EmoServiceMode getServiceMode();

    /**
     * Returns the name of the EmoDB cluster.
     */
    String getCluster();

    /**
     * Returns the local host and port for normal client access.
     */
    HostAndPort getServiceHostAndPort();

    /**
     * Returns the local host and port for administrative access.
     */
    HostAndPort getAdminHostAndPort();

    /**
     * Returns the application version for EmoDB.
     */
    String getApplicationVersion();

    /**
     * Returns a Curator instance whose namespace is pre-configured to the server's root namespace.
     */
    CuratorFramework getCurator();
}
