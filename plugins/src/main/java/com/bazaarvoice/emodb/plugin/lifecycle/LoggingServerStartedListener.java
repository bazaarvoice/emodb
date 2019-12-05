package com.bazaarvoice.emodb.plugin.lifecycle;

import com.bazaarvoice.emodb.plugin.PluginServerMetadata;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple sample implementation of {@link ServerStartedListener} which logs a message when the server starts.
 */
public class LoggingServerStartedListener implements ServerStartedListener<Void> {

    private PluginServerMetadata _metadata;

    @Override
    public void init(Environment environment, PluginServerMetadata metadata, Void ignore) {
        _metadata = metadata;
    }

    @Override
    public void serverStarted() {
        Logger log = LoggerFactory.getLogger(getClass());
        log.info("Service started for cluster {}: service available on {}:{}, admin available on {}:{}",
                _metadata.getCluster(),
                _metadata.getServiceHostAndPort().getHost(),
                _metadata.getServiceHostAndPort().getPort(),
                _metadata.getAdminHostAndPort().getHost(),
                _metadata.getAdminHostAndPort().getPort());
    }
}
