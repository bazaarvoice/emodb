package com.bazaarvoice.emodb.table.db.astyanax;

import com.google.common.base.Supplier;
import com.netflix.astyanax.model.ByteBufferRange;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Storage configuration for a single table in Astyanax.  Usually there is exactly one storage configuration active
 * for a table at a time, but when moving a table in a live cluster there will be two active storage configurations:
 * a source and a destination.
 * Contrary to the name, AstyanaxStorage is not actually specific to Astyanax, and its use does not imply any dependency
 * on Astyanax itself.
 * TODO: Rename class to "TableStorage" as it has nothing to do with Astyanax
 */
public class AstyanaxStorage {
    private final long _uuid;
    private final int _shardsLog2;  // either 2^4=16 shards or 2^8 = 256 shards
    private final boolean _readsAllowed;
    private final Supplier<Placement> _placement;
    private final String _placementName;

    public AstyanaxStorage(long uuid, int shardsLog2, boolean readsAllowed, String placementName,
                           Supplier<Placement> placement) {
        _uuid = uuid;
        _shardsLog2 = shardsLog2;
        _readsAllowed = readsAllowed;
        _placementName = checkNotNull(placementName, "placementName");
        _placement = checkNotNull(placement, "placement");
    }

    public boolean getReadsAllowed() {
        return _readsAllowed;
    }

    /**
     * Test whether a given UUID matches this storage configuration.  Avoids exposing the actual uuid to callers,
     * preventing callers from depending too strongly on the internals of table layout in Cassandra.
     */
    public boolean hasUUID(long uuid) {
        return _uuid == uuid;
    }

    public Placement getPlacement() {
        // Delay getting the actual placement details until we need them.  They may be for a foreign data center that
        // we don't have direct access to but we need to be able to manipulate the Table metadata.
        return _placement.get();
    }

    public String getPlacementName() {
        return _placementName;
    }

    public ByteBuffer getRowKey(String contentKey) {
        return RowKeyUtils.getRowKey(_uuid, _shardsLog2, contentKey);
    }

    public Iterator<ByteBufferRange> scanIterator(@Nullable String fromKeyExclusive) {
        return RowKeyUtils.scanIterator(_uuid, _shardsLog2, fromKeyExclusive);
    }

    public ByteBufferRange getSplitRange(ByteBufferRange range, @Nullable String fromKey, String splitDescription) {
        return RowKeyUtils.getSplitRange(_uuid, _shardsLog2, range, fromKey, splitDescription);
    }

    public boolean contains(ByteBuffer rowKey) {
        return RowKeyUtils.getTableUuid(rowKey) == _uuid;
    }

    // Low-level row key utilities

    public static String getContentKey(ByteBuffer rowKey) {
        return RowKeyUtils.getContentKey(rowKey);
    }

    public static int getShardId(ByteBuffer rowKey) {
        return RowKeyUtils.getShardId(rowKey);
    }

    public static int getShardSequence(ByteBuffer rowKey, int shardsLog2) {
        return RowKeyUtils.getShardSequence(rowKey, shardsLog2);
    }

    public static long getTableUuid(ByteBuffer rowKey) {
        return RowKeyUtils.getTableUuid(rowKey);
    }

    public static ByteBuffer getRowKeyRaw(int shardId, long uuid, String key) {
        return RowKeyUtils.getRowKeyRaw(shardId, uuid, key);
    }

    public static int compareKeys(ByteBuffer leftRowKey, ByteBuffer rightRowKey) {
        return RowKeyUtils.compareKeys(leftRowKey, rightRowKey);
    }
}
