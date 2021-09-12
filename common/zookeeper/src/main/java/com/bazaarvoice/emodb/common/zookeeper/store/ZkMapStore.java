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
                fireChanged(getKeyFromPath(path), ChangeType.ADD);
            }

            @Override
            public void onNodeRemoved(String path, T node) {
                fireChanged(getKeyFromPath(path), ChangeType.REMOVE);
            }

            @Override
            public void onNodeUpdated(String path, T node) {
                fireChanged(getKeyFromPath(path), ChangeType.CHANGE);
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
        // The key may contain special characters which are invalid in ZooKeeper paths.  Encode them
        String encodedKey = encodeKey(key);
        String path = ZKPaths.makePath(_zkPath, encodedKey);
        PathUtils.validatePath(path);
        return path;
    }

    private String encodeKey(String key) {
        StringBuilder encodedKey = null;
        int lastEncodedCharPos = -1;
        int length = key.length();
        for (int i=0; i < length; i++) {
            char c = key.charAt(i);
            if (c == '.'            // May be interpreted as relative path
                    || c == '%'     // Collides with encoding scheme
                    || isUnsupportedCharacter(c)) {
                if (lastEncodedCharPos == -1) {
                    // This is the first character that has been encoded
                    encodedKey = new StringBuilder();
                }
                encodedKey.append(key.substring(lastEncodedCharPos + 1, i));
                encodedKey.append("%").append(String.format("%04x", (int) c));
                lastEncodedCharPos = i;
            }
        }

        if (lastEncodedCharPos == -1) {
            return key;
        }

        encodedKey.append(key.substring(lastEncodedCharPos+1));
        return encodedKey.toString();
    }

    private boolean isUnsupportedCharacter(char c) {
        return c > '\u0000' && c < '\u001f'
                || c > '\u007f' && c < '\u009F'
                || c > '\ud800' && c < '\uf8ff'
                || c > '\ufff0' && c < '\uffff';
    }

    private String decodeKey(String key) {
        int i = key.indexOf('%');
        if (i == -1) {
            return key;
        }

        StringBuilder decodedKey = new StringBuilder();
        int endOfLastEncodedChar = 0;

        do {
            decodedKey.append(key.substring(endOfLastEncodedChar, i));
            endOfLastEncodedChar = i+5;
            decodedKey.append((char) Integer.parseInt(key.substring(i+1, endOfLastEncodedChar), 16));
            i = key.indexOf('%', endOfLastEncodedChar);
        } while (i != -1);

        decodedKey.append(key.substring(endOfLastEncodedChar));
        return decodedKey.toString();
    }

    private String getKeyFromPath(String path) {
        return decodeKey(ZKPaths.getNodeFromPath(path));
    }

    @Override
    public Map<String, T> getAll() {
        // Return an immutable copy since users should only be able to modify the map using set and remove.
        ImmutableMap.Builder<String, T> builder = ImmutableMap.builder();
        for (Map.Entry<String, T> entry : _nodeDiscovery.getNodes().entrySet()) {
            builder.put(getKeyFromPath(entry.getKey()), entry.getValue());
        }
        return builder.build();
    }

    @Override
    public Set<String> keySet() {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (String key : _nodeDiscovery.getNodes().keySet()) {
            builder.add(getKeyFromPath(key));
        }
        return builder.build();
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
