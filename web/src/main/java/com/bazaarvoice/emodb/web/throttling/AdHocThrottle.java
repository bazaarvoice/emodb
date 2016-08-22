package com.bazaarvoice.emodb.web.throttling;

import com.google.common.base.Objects;
import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An ad-hoc throttle consists of two attributes:
 *
 * <ol>
 *     <li>A limit on the number of concurrent requests</li>
 *     <li>An expiration date, after which requests should be unthrottled</li>
 * </ol>
 */
public class AdHocThrottle {
    // Singleton instance which represents no throttling
    private final static AdHocThrottle UNLIMITED = new AdHocThrottle(Integer.MAX_VALUE, DateTime.now().plusYears(1000));

    private final int _limit;
    private final DateTime _expiration;

    private AdHocThrottle(int limit, DateTime expiration) {
        checkArgument(limit >= 0, "limit cannot be negative");
        _limit = limit;
        _expiration = checkNotNull(expiration, "expiration");
    }

    public static AdHocThrottle create(int limit, DateTime expiration) {
        // If the throttle is unlimited or already expired then return the unlimited throttle.
        if (limit == Integer.MAX_VALUE || checkNotNull(expiration, "expiration").isBeforeNow()) {
            return unlimitedInstance();
        }
        return new AdHocThrottle(limit, expiration);
    }

    public static AdHocThrottle unlimitedInstance() {
        return UNLIMITED;
    }

    public int getLimit() {
        return _limit;
    }

    public DateTime getExpiration() {
        return _expiration;
    }

    public boolean isUnlimited() {
        return _limit == Integer.MAX_VALUE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AdHocThrottle)) {
            return false;
        }

        AdHocThrottle that = (AdHocThrottle) o;

        return _limit == that._limit && Objects.equal(_expiration, that._expiration);
    }

    @Override
    public int hashCode() {
        return  Objects.hashCode(_limit, _expiration);
    }

    public String toString() {
        return Objects.toStringHelper(this)
                .add("limit", _limit)
                .add("expiration", _expiration)
                .toString();
    }
}
