package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.zookeeper.store.ZkValueSerializer;
import java.time.Instant;

public class ZKDataStoreMinSplitSizeSerializer implements ZkValueSerializer<DataStoreMinSplitSize> {
    @Override
    public String toString(DataStoreMinSplitSize value) {
        return String.format("%d,%s", value.getMinSplitSize(), value.getExpirationTime());

    }

    @Override
    public DataStoreMinSplitSize fromString(String string) {
        if (string == null) {
            return null;
        }

        int comma = string.indexOf(',');
        if (comma <= 0) {
            throw new IllegalArgumentException("Min split size value cannot be parsed: " + string);
        }
        int minSplitSize = Integer.parseInt(string.substring(0, comma));
        Instant expirationTime = Instant.parse(string.substring(comma+1));
        return new DataStoreMinSplitSize(minSplitSize, expirationTime);    }
}
