package com.bazaarvoice.emodb.common.dropwizard.log;

import org.slf4j.Logger;

/**
 * Limits the rate that errors are logged for situations where, if something goes wrong, it's likely to go wrong many
 * times per second and all we're interested in is whether or not the error is still occurring.
 */
public interface RateLimitedLogFactory {
    /**
     * Returns a wrapper around the specified {@code Logger} object that limits error events to at most once per interval,
     * where the interval depends on the log factory implementation.
     */
    RateLimitedLog from(Logger log);
}
