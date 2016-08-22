package com.bazaarvoice.emodb.plugin.stash;

import com.bazaarvoice.emodb.plugin.PluginServerMetadata;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Simple sample implementation of {@link StashStateListener} which logs a message for each event.
 */
public class LoggingStashStateListener implements StashStateListener<Void> {
    Logger _log = LoggerFactory.getLogger(getClass());

    @Override
    public void init(Environment environment, PluginServerMetadata pluginServerMetadata, Void ignore) {
        // no-op
    }

    @Override
    public void announceStashParticipation() {
        _log.info("Server available to participate in Stash");
    }

    @Override
    public void stashStarted(StashMetadata info) {
        _log.info("Stash started: {}", info.getId());
    }

    @Override
    public void stashCompleted(StashMetadata info, Date completeTime) {
        _log.info("Stash complete: {}", info.getId());
    }

    @Override
    public void stashCanceled(StashMetadata info, Date canceledTime) {
        _log.info("Stash canceled: {}", info.getId());
    }
}
