package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.nio.BufferUtils;
import com.google.common.base.Charsets;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.util.ByteBufferRangeImpl;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

class RowKeyUtils {
    static final int LEGACY_SHARDS_LOG2 = 8;   // 256 shards.  New tables should override with 16 shards.

    static final int NUM_SHARDS_UNKNOWN = -1;

    // Changing the hash function will invalidate all existing shards.
    private static final HashFunction HASH_FN = Hashing.murmur3_32();

    static ByteBuffer getRowKey(long tableUuid, int shardsLog2, String contentKey) {
        // Don't allow a tableUuid of 0xffffffffffffffff since we can't use it in a range query for all rows between
        // (tableUuid, tableUuid+1) since tableUuid+1 overflows using unsigned math per the ByteOrderedPartitioner.
        checkArgument(tableUuid != -1);
        checkArgument(shardsLog2 >= 0 && shardsLog2 <= 8);  // Must fit in one byte.

        // The content key can't be the empty string.  We depend on that to be able to do exclusive table range queries
        // because it guarantees that getScanPrefix() never returns a value that matches a row key exactly.
        checkArgument(contentKey.length() > 0);
        byte[] contentKeyBytes = contentKey.getBytes(Charsets.UTF_8);
        int contentKeyLength = contentKeyBytes.length;

        // The row key byte buffer contains a 9-byte "table prefix" followed by a UTF-8-encoded contentKey.
        //
        // The 9-byte table prefix contains a shard identifier and a 64-bit table uuid:
        //   0 - 0: 8-bit shard identifier
        //   1 - 8: 64-bit table uuid
        //
        // The next portion is the UTF-8-encoded content key.
        //
        // The shard identifier serves to spread content for a given table across multiple prefixes to avoid hot
        // spots with the Cassandra ByteOrderedPartitioner.  The shard identifier is computed by using the bottom 8
        // bits of a hash of the rest of the bytes in the row key (table uuid + contentKey).  As a result, all content
        // for a given table can be fetched using 2^8=256 range queries.  This ability to enumerate all content for a
        // table is especially important when fetching input for Hadoop jobs.
        //
        // The table uuid allows content from multiple tables to be stored in the same column family.  Each column
        // family requires (as of Cassandra 1.1.1) at least 1MB of memory in every Cassandra node, so there's too much
        // overhead to create a column family per table, since we expect 1000s to 10,000s of tables.  Instead, we
        // store many tables in the same column family and separate them using the table row key prefix.  We use a uuid
        // table prefix instead of the String table name so it's possible to DROP a table then CREATE a new table with
        // the same name and not worry about data from before the DROP being visible after the CREATE, since a new uuid
        // is generated for each CREATE.  This makes it straightforward to delete the DROP'ed data lazily, which is
        // important in an eventually consistent world.  Also, using a uuid makes it cheap to support long table names--
        // no matter how long the table name is, the table-related portion of each row key is always a fixed 16 bytes.

        // Assemble a single array which is "1 byte shard id + 8 byte table uuid + n-byte content key".
        ByteBuffer rowKey = ByteBuffer.allocate(1 + 8 + contentKeyLength);
        rowKey.position(1);  // skip the shard id
        rowKey.putLong(tableUuid);
        rowKey.put(contentKeyBytes);

        // Use the bottom 8 bits of the 32-bit murmur3 algorithm as the shard identifier
        int hash = HASH_FN.hashBytes(rowKey.array(), rowKey.arrayOffset() + 1, 8 + contentKeyLength).asInt();
        rowKey.put(0, mix(hash, tableUuid, shardsLog2));

        rowKey.flip();
        return rowKey;
    }

    /**
     * Constructs a row key when the row's shard ID is already known, which is rare.  Generally this is used for
     * range queries to construct the lower or upper bound for a query, so it doesn't necessarily need to produce
     * a valid row key.
     */
    static ByteBuffer getRowKeyRaw(int shardId, long tableUuid, byte[] contentKeyBytes) {
        checkArgument(shardId >= 0 && shardId < 256);

        // Assemble a single array which is "1 byte shard id + 8 byte table uuid + n-byte content key".
        ByteBuffer rowKey = ByteBuffer.allocate(9 + contentKeyBytes.length);
        rowKey.put((byte) shardId);
        rowKey.putLong(tableUuid);
        rowKey.put(contentKeyBytes);

        rowKey.flip();
        return rowKey;
    }

    static ByteBuffer getRowKeyRaw(int shardId, long tableUuid, String contentKey) {
        return getRowKeyRaw(shardId, tableUuid, contentKey.getBytes(Charsets.UTF_8));
    }

    private static ByteBuffer getScanPrefix(int shardId, long tableUuid) {
        checkArgument(shardId >= 0 && shardId < 256);

        // Assemble an array which is "1 byte shard id + 8 byte table uuid".
        ByteBuffer rowKey = ByteBuffer.allocate(9);
        rowKey.put((byte) shardId);
        rowKey.putLong(tableUuid);

        rowKey.flip();
        return rowKey;
    }

    static int getShardId(ByteBuffer rowKey) {
        // Return the very first byte that indicates the shard id.
        checkArgument(rowKey.hasRemaining());
        return rowKey.get(rowKey.position()) & 0xff;
    }

    static int getShardSequence(ByteBuffer rowKey, int shardsLog2) {
        // If the shard ID doesn't use all 8 bytes then the low bytes are padded with deterministically random bits
        // per table.  This method returns the shard's ID as an integer between [0, 2^shardsLog2).
        return getShardId(rowKey) >> (8 - shardsLog2);
    }

    static long getTableUuid(ByteBuffer rowKey) {
        // Return the table UUID in bytes 1-8 inclusive
        return rowKey.getLong(rowKey.position() + 1);
    }

    static String getContentKey(ByteBuffer rowKey) {
        // Skip the 9-byte prefix
        return BufferUtils.getString(rowKey, 9, rowKey.remaining() - 9, Charsets.UTF_8);
    }

    static Iterator<ByteBufferRange> scanIterator(final long tableUuid, final int shardsLog2, @Nullable final String fromKey) {
        // Check that tableUuid + 1 won't overflow w/the unsigned math of ByteOrderedPartitioner
        checkArgument(tableUuid != -1);
        checkArgument(shardsLog2 >= 0 && shardsLog2 <= 8);  // Must fit in one byte.

        return new UnmodifiableIterator<ByteBufferRange>() {
            private String _from = fromKey;
            private int _shardId = mix(0, tableUuid, shardsLog2);

            @Override
            public boolean hasNext() {
                return _shardId < 256;
            }

            @Override
            public ByteBufferRange next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                ByteBuffer start;
                if (_from != null) {
                    start = getRowKey(tableUuid, shardsLog2, _from);
                    _from = null;
                    _shardId = getShardId(start);
                } else {
                    start = getScanPrefix(_shardId, tableUuid);
                }
                ByteBuffer end = getScanPrefix(_shardId, tableUuid + 1);
                _shardId += 1 << (8 - shardsLog2);  // Skip 2^(8-shardsLog2) since only the top shardsLog2 bits vary.
                return new ByteBufferRangeImpl(start, end, -1, false);
            }
        };
    }

    static ByteBufferRange getSplitRange(long tableUuid, int shardsLog2, ByteBufferRange range,
                                         @Nullable String fromKey, String splitDescription) {
        ByteBuffer start = range.getStart();
        ByteBuffer end = range.getEnd();

        // Validate that the start and end belong to the same shard of the table.  Otherwise a range query could
        // return data from another table, which would be bad...
        int shardId = getShardId(start);
        if ((mix(shardId, tableUuid, shardsLog2) & 0xff) != shardId) {
            throw new IllegalArgumentException(format("Invalid split range: %s", splitDescription));
        }
        ByteBuffer min = getScanPrefix(shardId, tableUuid);
        ByteBuffer max = getScanPrefix(shardId, tableUuid + 1);
        if (!lessThanOrEqual(min, start) || !lessThanOrEqual(start, end) || !lessThanOrEqual(end, max)) {
            throw new IllegalArgumentException(format("Invalid split range: %s", splitDescription));
        }

        // If specified, the fromKey overrides the start.  But verify it's within the split.
        if (fromKey != null) {
            ByteBuffer from = getRowKey(tableUuid, shardsLog2, fromKey);
            if (!lessThanOrEqual(start, from) || !lessThanOrEqual(from, end)) {
                throw new IllegalArgumentException(format("Invalid from key '%s', does not belong to the specified split: %s", fromKey, splitDescription));
            }
            range = new ByteBufferRangeImpl(from, end, -1, false);
        }

        return range;
    }

    public static int compareKeys(ByteBuffer leftRowKey, ByteBuffer rightRowKey) {
        return BufferUtils.compareUnsigned(leftRowKey, rightRowKey);
    }

    private static boolean lessThanOrEqual(ByteBuffer low, ByteBuffer high) {
        return compareKeys(low, high) <= 0;
    }

    private static byte mix(int hash, long tableUuid, int shardsLog2) {
        // The most significant shardsLog2 bits are taken from 'hash' and the least significant 8-shardsLog2 bits
        // are taken from 'tableUuid'.
        // For eg. shardsLog2==4, the top 4 bits vary by row key and the bottom 4 bits are the same for all keys in
        // a given table.  The bottom 4 bits are copied from the table uuid so that keys from different tables vary,
        // ensuring a reasonably equal distribution around the ring.  To understand why this is important, consider
        // the extreme example of a 256-server Cassandra ring with replication factor=1 and token assignments of
        // 00, 01, 02, ... FE, FF.  If the bottom 4 bits of the shardId were constant, eg. always 0, then there
        // would be only 16 distinct values for the first byte and only 16 of the 256 servers would have data.
        // Note: changing this mix function may invalidate all existing shards.
        int mask = -1 << (8 - shardsLog2);
        long mixed = (hash & mask) | ((int) tableUuid & ~mask);
        return (byte) (mixed & 0xff);
    }

    static int computeShardsLog2(int numShards, String source) {
        checkArgument(numShards >= 1 && numShards <= 256, "Shard count must be between 1 and 256: %s (uuid=%s)", numShards, source);
        checkArgument(Integer.bitCount(numShards) == 1, "Shard count must be a power of 2: %s (uuid=%s)", numShards, source);
        return Integer.numberOfTrailingZeros(numShards);  // Equivalent to log2(n) when n is a power of 2
    }
}
