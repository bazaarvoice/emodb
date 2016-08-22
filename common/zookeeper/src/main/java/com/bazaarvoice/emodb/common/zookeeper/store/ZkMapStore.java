package com.bazaarvoice.emodb.common.zookeeper.store;

import com.bazaarvoice.curator.recipes.NodeDiscovery;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.dropwizard.lifecycle.Managed;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.common.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Stores a Map<String, T> in ZooKeeper, watches for changes made by other servers, and caches
 * values locally for fast access. Uses NodeDiscovery to cache values.
 */
public class ZkMapStore<T> implements MapStore<T>, Managed {
    private static final Logger _log = LoggerFactory.getLogger(ZkMapStore.class);

    private final CuratorFramework _curator;
    private final String _zkPath;
    private final ZkValueSerializer<T> _serializer;
    private final NodeDiscovery<T> _nodeDiscovery;
    private final List<MapStoreListener> _listeners = Lists.newCopyOnWriteArrayList();

    public ZkMapStore(CuratorFramework curator, String zkPath, ZkValueSerializer<T> serializer) {
        _curator = checkNotNull(curator, "curator");
        _zkPath = checkNotNull(zkPath, "zkPath");
        _serializer = checkNotNull(serializer, "serializer");
        _nodeDiscovery = new NodeDiscovery<>(curator, zkPath, new NodeDiscovery.NodeDataParser<T>() {
            @Override
            public T parse(String s, byte[] bytes) {
                return fromZkBytes(bytes);
            }
        });
        _nodeDiscovery.addListener(new NodeDiscovery.NodeListener<T>() {
            @Override
            public void onNodeAdded(String path, T node) {
                fireChanged(ZKPaths.getNodeFromPath(path), ChangeType.ADD);
            }

            @Override
            public void onNodeRemoved(String path, T node) {
                fireChanged(ZKPaths.getNodeFromPath(path), ChangeType.REMOVE);
            }

            @Override
            public void onNodeUpdated(String path, T node) {
                fireChanged(ZKPaths.getNodeFromPath(path), ChangeType.CHANGE);
            }
        });

    }

    @Override
    public void start() throws Exception {
        // Create the ZooKeeper node (persistent, not ephemeral)
        createNode(_zkPath);

        _nodeDiscovery.start();
    }

    private void createNode(String zkPath) throws Exception {
        try {
            _curator.create().creatingParentsIfNeeded().forPath(zkPath);
        } catch (KeeperException.NodeExistsException e) {
            // Don't care
        }
    }

    @Override
    public void stop() throws Exception {
        _nodeDiscovery.close();
    }

    private String toPath(String key) {
        checkArgument(key.indexOf('/') == -1, "Keys may not contain '/'.");
        String path = ZKPaths.makePath(_zkPath, key);
        PathUtils.validatePath(path);
        return path;
    }

    @Override
    public Map<String, T> getAll() {
        // Return an immutable copy since users should only be able to modify the map using set and remove.
        ImmutableMap.Builder<String, T> builder = ImmutableMap.builder();
        for (Map.Entry<String, T> entry : _nodeDiscovery.getNodes().entrySet()) {
            builder.put(ZKPaths.getNodeFromPath(entry.getKey()), entry.getValue());
        }
        return builder.build();
    }

    @Override
    public Set<String> keySet() {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (String key : _nodeDiscovery.getNodes().keySet()) {
            builder.add(ZKPaths.getNodeFromPath(key));
        }
        return builder.build();
    }

    @Override
    public String getZkPath() {
        return _zkPath;
    }

    @Nullable
    @Override
    public T get(String key) {
        return _nodeDiscovery.getNodes().get(toPath(key));
    }

    @Override
    public void set(String key, T value) throws Exception {
        if (getAll().containsKey(key)) {
            _curator.setData().forPath(toPath(key), toZkBytes(value));
        } else {
            _curator.create().forPath(toPath(key), toZkBytes(value));
        }
    }

    @Override
    public void remove(String key) throws Exception {
        try {
            _curator.delete().forPath(toPath(key));
        } catch (KeeperException.NoNodeException e) {
            // Don't care
        }
    }

    private byte[] toZkBytes(T value) {
        if (value == null) {
            return new byte[0];
        }
        String string = _serializer.toString(value);
        return string.getBytes(Charsets.UTF_8);
    }

    private T fromZkBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        try {
            String string = new String(bytes, Charsets.UTF_8);
            return _serializer.fromString(string);
        } catch (Exception e) {
            _log.error("Exception trying to parse value from ZK path {}: {}", _zkPath, e);
            return null;
        }
    }

    @Override
    public void addListener(MapStoreListener listener) {
        _listeners.add(listener);
    }

    @Override
    public void removeListener(MapStoreListener listener) {
        _listeners.remove(listener);
    }

    private void fireChanged (String key, ChangeType changeType) {
        for (MapStoreListener listener : _listeners) {
            try {
                listener.entryChanged(key, changeType);
            } catch (Throwable t) {
                _log.error("Listener threw an unexpected exception.", t);
            }
        }

    }

    @Override
    public String toString() {
        return Maps.transformValues(getAll(), new Function<T, Object>() {
            @Override
            public Object apply(T value) {
                return _serializer.toString(value);
            }
        }).toString();
    }
}
