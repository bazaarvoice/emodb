package com.bazaarvoice.emodb.web.scanner.notifications;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Scan count notifier to send notifications to multiple notifiers.
 */
public class MultiScanCountListener implements ScanCountListener {

    private final Logger _log = LoggerFactory.getLogger(MultiScanCountListener.class);

    private final Collection<ScanCountListener> _delegates;

    public static ScanCountListener combine(Collection<ScanCountListener> notifiers) {
        requireNonNull(notifiers, "notifiers");
        checkArgument(!notifiers.isEmpty(), "At least one notifier is required");
        if (notifiers.size() == 1) {
            return Iterables.getOnlyElement(notifiers);
        }
        return new MultiScanCountListener(notifiers);
    }

    public MultiScanCountListener(Collection<ScanCountListener> delegates) {
        _delegates = delegates;
    }

    @Override
    public void pendingScanCountChanged(int pending) {
        for (ScanCountListener delegate : _delegates) {
            try {
                delegate.pendingScanCountChanged(pending);
            } catch (Exception e) {
                _log.error("Failed to update pending scan counts for delegate", e);
            }
        }
    }

    @Override
    public void activeScanCountChanged(int active) {
        for (ScanCountListener delegate : _delegates) {
            try {
                delegate.activeScanCountChanged(active);
            } catch (Exception e) {
                _log.error("Failed to update active scan counts for delegate", e);
            }
        }
    }
}
