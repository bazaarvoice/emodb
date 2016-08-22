package com.bazaarvoice.emodb.web.scanner.notifications;

import com.bazaarvoice.emodb.plugin.PluginServerMetadata;
import com.bazaarvoice.emodb.plugin.stash.StashMetadata;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.google.common.collect.Iterables;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Scan state notifier to send notifications to multiple notifiers.
 */
public class MultiStashStateListener implements StashStateListener<Void> {

    private final Logger _log = LoggerFactory.getLogger(MultiStashStateListener.class);

    private final Collection<StashStateListener> _delegates;

    public static StashStateListener combine(Collection<StashStateListener> listeners) {
        checkNotNull(listeners, "notifiers");
        checkArgument(!listeners.isEmpty(), "At least one notifier is required");
        if (listeners.size() == 1) {
            return Iterables.getOnlyElement(listeners);
        }
        return new MultiStashStateListener(listeners);
    }

    private MultiStashStateListener(Collection<StashStateListener> delegates) {
        _delegates = delegates;
    }

    @Override
    public void init(Environment environment, PluginServerMetadata metadata, Void ignore) {
        // All contained instances should already be initialized, so do nothing
    }

    @Override
    public void announceStashParticipation() {
        for (StashStateListener listener : _delegates) {
            try {
                listener.announceStashParticipation();
            } catch (Exception e) {
                _log.warn("Listener threw uncaught exception", e);
            }
        }
    }

    @Override
    public void stashStarted(StashMetadata info) {
        for (StashStateListener listener : _delegates) {
            try {
                listener.stashStarted(info);
            } catch (Exception e) {
                _log.warn("Listener threw uncaught exception", e);
            }
        }
    }

    @Override
    public void stashCompleted(StashMetadata info, Date competeTime) {
        for (StashStateListener listener : _delegates) {
            try {
                listener.stashCompleted(info, competeTime);
            } catch (Exception e) {
                _log.warn("Listener threw uncaught exception", e);
            }
        }
    }

    @Override
    public void stashCanceled(StashMetadata info, Date canceledTime) {
        for (StashStateListener notifier : _delegates) {
            try {
                notifier.stashCanceled(info, canceledTime);
            } catch (Exception e) {
                _log.warn("Listener threw uncaught exception", e);
            }
        }
    }
}
