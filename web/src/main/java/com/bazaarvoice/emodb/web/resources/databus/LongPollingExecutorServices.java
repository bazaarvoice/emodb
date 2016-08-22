package com.bazaarvoice.emodb.web.resources.databus;

import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * POJO for maintaining the executor services used for long polling.
 */
public class LongPollingExecutorServices {
    private final ScheduledExecutorService _poller;
    private final ScheduledExecutorService _keepAlive;

    public LongPollingExecutorServices(ScheduledExecutorService poller, ScheduledExecutorService keepAlive) {
        _poller = checkNotNull(poller, "Long poll polling service");
        _keepAlive = checkNotNull(keepAlive, "Long poll keep-alive service");
    }

    public ScheduledExecutorService getPoller() {
        return _poller;
    }

    public ScheduledExecutorService getKeepAlive() {
        return _keepAlive;
    }
}
