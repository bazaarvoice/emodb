package com.bazaarvoice.emodb.common.zookeeper.store;

/** Formats a boolean in ZooKeeper as a human-readable string for transparency, easy debugging. */
public class ZkBooleanSerializer implements ZkValueSerializer<Boolean> {
    @Override
    public String toString(Boolean value) {
        return value.toString();
    }

    @Override
    public Boolean fromString(String string) {
        return Boolean.parseBoolean(string);
    }
}
