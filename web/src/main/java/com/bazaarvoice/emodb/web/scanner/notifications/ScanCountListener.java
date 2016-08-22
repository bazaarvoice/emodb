package com.bazaarvoice.emodb.web.scanner.notifications;

/**
 * Interface to send notifications whenever the number of active and/or pending scans has changed.
 */
public interface ScanCountListener {

    void pendingScanCountChanged(int pending);

    void activeScanCountChanged(int active);
}
