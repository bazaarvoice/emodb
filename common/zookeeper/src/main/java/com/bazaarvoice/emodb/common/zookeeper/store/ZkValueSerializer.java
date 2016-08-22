package com.bazaarvoice.emodb.common.zookeeper.store;

/**
 * Convert values to and from strings.  Values are stored in ZooKeeper in strings for transparency
 * and ease of debugging.
 */
public interface ZkValueSerializer<T> {
    String toString(T value);

    T fromString(String string);
}
