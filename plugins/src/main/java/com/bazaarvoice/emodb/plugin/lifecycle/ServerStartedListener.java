package com.bazaarvoice.emodb.plugin.lifecycle;

import com.bazaarvoice.emodb.plugin.Plugin;

/**
 * Listener interface for receiving notification once the server has started.  Although the Environment instance
 * provided to {@link #init(io.dropwizard.setup.Environment, com.bazaarvoice.emodb.plugin.PluginServerMetadata, Object)}
 * can be used to perform all manner of EmoDB customization, such as adding custom Jersey filters, to prevent
 * interference with the core EmoDB system implementations are encouraged to limit its use to additive rather
 * than mutative changes.  For example, one possible use is to register the server with a service registry.
 */
public interface ServerStartedListener<T> extends Plugin<T> {

    /**
     * Called after the server is fully initialized and all resources are available.
     */
    void serverStarted();
}
