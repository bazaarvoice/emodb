package com.bazaarvoice.emodb.table.db.tableset;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.table.db.DroppedTableException;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * TableSerializer which caches tables locally and in ZooKeeper to provide a consistent view for all clients
 * across the cluster.
 *
 * By default the ZooKeeper path is not cleaned up when this instance is closed, although this can be configured
 * by calling {@link #setCleanupOnClose(boolean)}.  If this is not enabled then the caller should ensure that
 * {@link DistributedTableSerializer#cleanup(org.apache.curator.framework.CuratorFramework, String)} is called
 * at some point after the TableSet is no longer being used.
 */
public class DistributedTableSerializer implements TableSerializer, Closeable {

    // Special values to denote when a table is unknown or dropped
    private final static int UNKNOWN_TABLE = -1;
    private final static int DROPPED_TABLE = -2;

    private final static Logger _log = LoggerFactory.getLogger(DistributedTableSerializer.class);

    private final TableSerializer _source;
    private final CuratorFramework _curator;
    private final String _basePath;
    private final EnsurePath _ensurePath;

    private boolean _cleanupOnClose = false;

    public DistributedTableSerializer(TableSerializer tableSerializer, CuratorFramework curator, String basePath) {
        _source = requireNonNull(tableSerializer, "tableSerializer");
        _curator = requireNonNull(curator, "curator");
        _basePath = requireNonNull(basePath, "basePath");
        _ensurePath = curator.newNamespaceAwareEnsurePath(basePath);
    }

    public DistributedTableSerializer(SerializingTableSet sourceTableSet, CuratorFramework curator, String basePath) {
        this(requireNonNull(sourceTableSet, "sourceTableSet").getTableSerializer(), curator, basePath);
    }

    public DistributedTableSerializer(TableSet sourceTableSet, CuratorFramework curator, String basePath) {
        this(narrowToSerializingTableSet(sourceTableSet), curator, basePath);
    }

    private static SerializingTableSet narrowToSerializingTableSet(TableSet tableSet) {
        requireNonNull(tableSet, "tableSet");
        checkArgument(tableSet instanceof SerializingTableSet, "TableSet must implement SerializingTableSet");
        //noinspection ConstantConditions
        return (SerializingTableSet) tableSet;
    }

    public void setCleanupOnClose(boolean cleanupOnClose) {
        _cleanupOnClose = cleanupOnClose;
    }

    @Override
    public Set<Long> loadAndSerialize(long uuid, OutputStream out)
            throws IOException, UnknownTableException, DroppedTableException {
        String path = ZKPaths.makePath(_basePath, Long.toString(uuid));

        try {
            // Ensure the base path exists.  This is a quick no-op on all but the first time it is called
            _ensurePath.ensure(_curator.getZookeeperClient());
            // Attempt to load the data from ZooKeeper, in case it has already been cached by another process
            return loadFromNode(path, out);
        } catch (KeeperException.NoNodeException e) {
            // Ok, we'll read it from the source and write it to ZooKeeper
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        try {
            return writeToNode(path, uuid, out);
        } catch (KeeperException.NodeExistsException e) {
            // Ok, there was a race condition and someone else wrote it first.  Return what they wrote.
        }

        try {
            return loadFromNode(path, out);
        } catch (KeeperException.NoNodeException e) {
            // This shouldn't happen, we only got this far because there was a conflict when we tried to write the node.
            throw Throwables.propagate(e);
        }
    }

    /**
     * Loads the table data from the path and writes it to the output stream.  Returns the set of UUIDs associated with
     * the table, just like {@link #loadAndSerialize(long, java.io.OutputStream)}.
     */
    private Set<Long> loadFromNode(String path, OutputStream out)
            throws KeeperException.NoNodeException {
        try {
            // Get the cached representation of this table
            byte[] data = _curator.getData().forPath(path);
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            int uuidCountOrExceptionCode = in.readInt();

            // A negative first integer indicates an exception condition.
            if (uuidCountOrExceptionCode < 0) {
                String exceptionJson = new String(data, 4, data.length - 4, Charsets.UTF_8);
                switch (uuidCountOrExceptionCode) {
                    case UNKNOWN_TABLE:
                        throw JsonHelper.fromJson(exceptionJson, UnknownTableException.class);
                    case DROPPED_TABLE:
                        throw JsonHelper.fromJson(exceptionJson, DroppedTableException.class);
                }
            }

            // Load the UUIDs for this table
            Set<Long> uuids = Sets.newHashSet();
            for (int i=0; i < uuidCountOrExceptionCode; i++) {
                uuids.add(in.readLong());
            }

            // Copy the remaining bytes as the content
            ByteStreams.copy(in, out);
            return uuids;
        } catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, KeeperException.NoNodeException.class);
            throw Throwables.propagate(t);
       }
    }

    /**
     * Defers loading the table to the underlying serializer.  Caches the result in ZooKeeper and passes it along
     * to the caller, just like {@link #loadAndSerialize(long, java.io.OutputStream)}.
     */
    private Set<Long> writeToNode(String path, long uuid, OutputStream out)
            throws KeeperException.NodeExistsException, IOException, UnknownTableException, DroppedTableException {
        ByteArrayOutputStream bufferOut = new ByteArrayOutputStream();
        byte[] tableContent;
        Set<Long> uuids;
        Exception exception = null;

        try {
            // Load the table content from the source
            uuids = _source.loadAndSerialize(uuid, bufferOut);
            tableContent = bufferOut.toByteArray();
        } catch (UnknownTableException | DroppedTableException e) {
            // Record the exception and store the JSON serialized exception as the table content
            exception = e;
            tableContent = JsonHelper.asJson(e).getBytes(Charsets.UTF_8);
            uuids = ImmutableSet.of();
        }

        int nodeContentLength = 4 + 8 * uuids.size() + tableContent.length;  // 4 byte uuid count + 8 bytes per uuid + tableContent
        bufferOut = new ByteArrayOutputStream(nodeContentLength);
        DataOutputStream nodeOut = new DataOutputStream(bufferOut);

        if (exception == null) {
            // Write the actual number of UUIDs associated with the table
            nodeOut.writeInt(uuids.size());
            for (long sourceUuid : uuids) {
                nodeOut.writeLong(sourceUuid);
            }
        } else {
            // Write the exception code (no UUIDs will be written so no size is required)
            nodeOut.writeInt(exception instanceof UnknownTableException ? UNKNOWN_TABLE : DROPPED_TABLE);
        }

        nodeOut.write(tableContent);

        byte[] nodeContent = bufferOut.toByteArray();

        try {
            // Write the content to ZooKeeper
            _curator.create().withMode(CreateMode.PERSISTENT).forPath(path, nodeContent);
            _log.debug("Wrote table for UUID {} to {}", uuid, path);
        } catch (Throwable t) {
            // If the node exists then there was a race condition with another server caching the same table.  Pass
            // to the caller so they can retry reading the value.
            Throwables.propagateIfInstanceOf(t, KeeperException.NodeExistsException.class);
            Throwables.propagateIfInstanceOf(t, IOException.class);
            throw Throwables.propagate(t);
        }

        if (exception != null) {
            // This will always raise the exception
            Throwables.propagateIfPossible(exception, UnknownTableException.class, DroppedTableException.class);
        }

        out.write(tableContent);
        return uuids;
    }

    @Override
    public Table deserialize(InputStream in)
            throws IOException {
        return _source.deserialize(in);
    }

    @Override
    public void close() {
        if (_cleanupOnClose) {
            cleanup(_curator, _basePath);
        }
    }

    public static void cleanup(CuratorFramework curator, String basePath) {
        // Get all children
        List<String> children;
        try {
            children = curator.getChildren().forPath(basePath);
        } catch (KeeperException.NoNodeException e) {
            // Node was never read nor written to, or another server has already deleted it
            return;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        // Delete all children
        for (String child : children) {
            String childPath = ZKPaths.makePath(basePath, child);
            try {
                curator.delete().forPath(childPath);
            } catch (KeeperException.NoNodeException e) {
                // Ok, another server deleted it first
            } catch (Exception e) {
                _log.warn("Failed to delete child node: {}", childPath, e);
            }
        }

        // Delete the base path if it is not empty
        try {
            curator.delete().forPath(basePath);
        } catch (KeeperException.NoNodeException e) {
            // Ok, another server deleted it first
        } catch (KeeperException.NotEmptyException e) {
            _log.info("Node not deleted because it is not empty: {}", basePath);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
