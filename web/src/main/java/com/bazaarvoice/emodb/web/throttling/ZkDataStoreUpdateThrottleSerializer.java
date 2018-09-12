package com.bazaarvoice.emodb.web.throttling;

import com.bazaarvoice.emodb.common.zookeeper.store.ZkValueSerializer;

import java.time.Instant;

/**
 * Simple serializer for storing {@link DataStoreUpdateThrottle} configurations in ZooKeeper.
 */
public class ZkDataStoreUpdateThrottleSerializer implements ZkValueSerializer<DataStoreUpdateThrottle> {

    @Override
    public String toString(DataStoreUpdateThrottle value) {
        return String.format("%.8f,%s", value.getRateLimit(), value.getExpirationTime());
    }

    @Override
    public DataStoreUpdateThrottle fromString(String string) {
        if (string == null) {
            return null;
        }

        int comma = string.indexOf(',');
        if (comma <= 0) {
            throw new IllegalArgumentException("Rate limit value cannot be parsed: " + string);
        }
        double rateLimit = Double.parseDouble(string.substring(0, comma));
        Instant expirationTime = Instant.parse(string.substring(comma+1));
        return new DataStoreUpdateThrottle(rateLimit, expirationTime);
    }
}
