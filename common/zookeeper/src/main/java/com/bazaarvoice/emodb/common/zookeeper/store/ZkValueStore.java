package com.bazaarvoice.emodb.common.zookeeper.store;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import io.dropwizard.lifecycle.Managed;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Stores a single value in ZooKeeper, watches for changes made by other servers in the same cluster, and caches
 * values locally for fast access.
 */
public class ZkValueStore<T> implements ValueStore<T>, Managed {
    private static final Logger _log = LoggerFactory.getLogger(ZkValueStore.class);

    private final CuratorFramework _curator;
    private final String _zkPath;
    private final ZkValueSerializer<T> _serializer;
    private final NodeCache _nodeCache;
    private final List<ValueStoreListener> _listeners = Lists.newCopyOnWriteArrayList();
    private final T _defaultValue;
    private volatile T _value;

    public ZkValueStore(CuratorFramework curator, String zkPath, ZkValueSerializer<T> serializer) {
        this(curator, zkPath, serializer, null);
    }

    public ZkValueStore(CuratorFramework curator, String zkPath, ZkValueSerializer<T> serializer, T defaultValue) {
        _curator = checkNotNull(curator, "curator");
        _zkPath = checkNotNull(zkPath, "zkPath");
        _serializer = checkNotNull(serializer, "serializer");
        _nodeCache = new NodeCache(curator, zkPath);
        _defaultValue = defaultValue;
        // Create node on instantiation
        // In practice, this creates a persistent zookeeper node without having to start the Managed resource.
        try {
            createNode();
        } catch (Exception e) {
            // Log a warning. We will try again when Managed is started
            _log.warn(format("Could not create node %s", _zkPath));
        }
    }

    @Override
    public void start() throws Exception {
        // Create the zookeeper node
        createNode();
        // Initial data load (avoid race conditions w/"NodeCache.start(true)")
        updateFromZkBytes(_curator.getData().forPath(_zkPath), _defaultValue);

        // Re-load the data and watch for changes.
        _nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                ChildData childData = _nodeCache.getCurrentData();
                if (childData != null) {
                    updateFromZkBytes(childData.getData(), _defaultValue);
                }
            }
        });
        _nodeCache.start();
    }

    private void createNode() throws Exception {
        // Create the ZooKeeper node (persistent, not ephemeral)
        try {
            _curator.create().creatingParentsIfNeeded().forPath(_zkPath, new byte[0]);
        } catch (KeeperException.NodeExistsException e) {
            // Don't care
        }
    }

    @Override
    public void stop() throws Exception {
        _nodeCache.close();
    }

    @Override
    public T get() {
        return _value;
    }

    @Override
    public void set(T value) throws Exception {
        // Eventually consistent.  To ensure things work the same when testing w/a single server vs. multiple
        // servers, get() won't return the value until it has round-tripped through ZooKeeper.
        _curator.setData().forPath(_zkPath, toZkBytes(value));
    }

    private byte[] toZkBytes(T value) {
        if (value == null) {
            return new byte[0];
        }
        String string = _serializer.toString(value);
        return string.getBytes(Charsets.UTF_8);
    }

    private void updateFromZkBytes(byte[] bytes, T defaultValue) {
        if (bytes == null || bytes.length == 0) {
            _value = defaultValue;
        } else {
            try {
                String string = new String(bytes, Charsets.UTF_8);
                _value = _serializer.fromString(string);
            } catch (Exception e) {
                _log.error("Exception trying to parse value from ZK path {}: {}", _zkPath, e);
            }
        }
        fireChanged();
    }

    @Override
    public void addListener(ValueStoreListener listener) {
        _listeners.add(listener);
    }

    @Override
    public void removeListener(ValueStoreListener listener) {
        _listeners.remove(listener);
    }

    private void fireChanged() {
        for (ValueStoreListener listener : _listeners) {
            try {
                listener.valueChanged();
            } catch (Throwable t) {
                _log.error("Listener threw an unexpected exception.", t);
            }
        }
    }

    @Override
    public String toString() {
        return _value != null ? _serializer.toString(_value) : "null";
    }
}
