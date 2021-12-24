package com.bazaarvoice.emodb.web.throttling;

import java.time.Instant;
import java.util.Objects;

import static java.util.Objects.hash;

/**
 * Throttle for updates to the DataStore.  This consists of two attributes:
 * <ol>
 *     <li>The rate limit in terms of updates per second.</li>
 *     <li>A expiration time after which the rate limit will no longer be enforced.</li>
 * </ol>
 */
public class DataStoreUpdateThrottle {

    private final double _rateLimit;
    private final Instant _expirationTime;

    public DataStoreUpdateThrottle(double rateLimit, Instant expirationTime) {
        _rateLimit = rateLimit;
        _expirationTime = expirationTime;
    }

    public double getRateLimit() {
        return _rateLimit;
    }

    public Instant getExpirationTime() {
        return _expirationTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataStoreUpdateThrottle)) {
            return false;
        }
        DataStoreUpdateThrottle that = (DataStoreUpdateThrottle) o;
        return that._rateLimit == _rateLimit &&
                Objects.equals(_expirationTime, that._expirationTime);
    }

    @Override
    public int hashCode() {
        return hash(_rateLimit, _expirationTime);
    }
}
