package com.bazaarvoice.emodb.plugin;

import io.dropwizard.setup.Environment;

/**
 * Base interface for all plugins.
 */
public interface Plugin<T> {

    /**
     * Initializes the plugin.  This method is called as part of the server's initialization.  Parts of EmoDB
     * may be started at the time this method is called but it is not guaranteed that the entire server has been
     * initialized and started.
     */
    void init(Environment environment, PluginServerMetadata metadata, T config);
}
