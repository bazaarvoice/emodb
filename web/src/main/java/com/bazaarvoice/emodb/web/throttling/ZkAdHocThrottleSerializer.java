package com.bazaarvoice.emodb.web.throttling;

import com.bazaarvoice.emodb.common.zookeeper.store.ZkTimestampSerializer;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkValueSerializer;
import org.joda.time.DateTime;

/**
 * Simple serializer for storing {@link AdHocThrottle} configurations in ZooKeeper.
 */
public class ZkAdHocThrottleSerializer implements ZkValueSerializer<AdHocThrottle> {

    private static final ZkTimestampSerializer TIMESTAMP_SERIALIZER = new ZkTimestampSerializer();

    @Override
    public String toString(AdHocThrottle throttle) {
        return String.format("%s,%s", throttle.getLimit(), TIMESTAMP_SERIALIZER.toString(throttle.getExpiration().getMillis()));
    }

    @Override
    public AdHocThrottle fromString(String string) {
        if (string == null) {
            return null;
        }
        try {
            int comma = string.indexOf(",");
            int limit = Integer.parseInt(string.substring(0, comma));
            DateTime expiration = new DateTime(TIMESTAMP_SERIALIZER.fromString(string.substring(comma + 1)));
            return AdHocThrottle.create(limit, expiration);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Throttle string must be of the form \"limit,expiration date\"");
        }
    }
}
