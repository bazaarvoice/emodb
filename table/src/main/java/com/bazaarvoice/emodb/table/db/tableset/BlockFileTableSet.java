package com.bazaarvoice.emodb.table.db.tableset;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.table.db.DroppedTableException;
import com.bazaarvoice.emodb.table.db.Table;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.apache.cassandra.utils.Pair;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.ByteBufferOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * TableSet implementation that writes tables to a file-backed memory cache after their first access to return a
 * consistent table on subsequent accesses.  All UUIDs associated with the table are mapped on the first read, so
 * looking up a table by one UUID an then subsequently by a different UUID should always return equivalent tables.
 *
 * Tables are written to memory in blocks and the blocks are paged to disk when the maximum configured amount of
 * memory has been used.  This allows for a reasonably configurable memory bound with only a small consistent
 * growth with the number of tables.
 */
public class BlockFileTableSet extends AbstractSerializingTableSet {
    private static final Logger _log = LoggerFactory.getLogger(BlockFileTableSet.class);

    // Minimum size for a block.  Since each table must fit in a single block a reasonable minimum must be enforced
    private static final int MIN_BLOCK_SIZE = 5000;
    private static final int DEFAULT_BLOCK_SIZE = 5 * 1024 * 1024;  // 5 Mb
    private static final int DEFAULT_BUFFER_COUNT = 6;

    // Size of each block
    private final int _blockSize;
    // Maximum number of buffers
    private final int _bufferCount;
    // List of all blocks
    private final List<TableBlock> _blocks = Lists.newArrayList(new TableBlock(0));
    // Maps UUIDs to the index in the blocks where the table is located
    private Map<Long, Integer> _fileIndexByUuid = Maps.newConcurrentMap();

    public BlockFileTableSet(TableSerializer tableSerializer) {
        this(tableSerializer, DEFAULT_BLOCK_SIZE, DEFAULT_BUFFER_COUNT);
    }

    public BlockFileTableSet(TableSerializer tableSerializer, int blockSize, int bufferCount) {
        super(tableSerializer);
        checkArgument(blockSize >= MIN_BLOCK_SIZE, "buffer size < %d", MIN_BLOCK_SIZE);
        checkArgument(bufferCount >= 1, "buffer count < 1");
        _blockSize = blockSize;
        _bufferCount = bufferCount;
    }

    @Override
    public Table getByUuid(long uuid)
            throws UnknownTableException, DroppedTableException {
        try {
            Integer index = _fileIndexByUuid.get(uuid);
            if (index == null) {
                synchronized (this) {
                    index = _fileIndexByUuid.get(uuid);
                    if (index == null) {
                        // First time this UUID has been seen, so load the table
                        index = loadTable(uuid);
                    }
                }
            }
            return readTable(index);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Loads a table from the source and places it into the next available space in the table blocks.
     * Once the table is written it returns the index where the table is located for future reads.
     */
    private int loadTable(long uuid)
            throws IOException {
        int bufferIndex = -1;
        Set<Long> uuids = null;

        // Always attempt to append to the last block first.
        int blockIndex = _blocks.size() - 1;
        TableBlock lastBlock = _blocks.get(blockIndex);

        // Even though this is a while loop it will run at most twice: once if there is room in the current final
        // block, and a second time if there wasn't and a new block needed to be allocated.
        while (bufferIndex == -1) {
            Pair<Integer, Set<Long>> bufferIndexAndUuuids = lastBlock.writeTable(uuid);
            bufferIndex = bufferIndexAndUuuids.left;
            if (bufferIndex == -1) {
                blockIndex++;
                lastBlock = new TableBlock(blockIndex * _blockSize);
                _blocks.add(lastBlock);
            } else {
                uuids = bufferIndexAndUuuids.right;
            }
        }

        int index = toIndex(blockIndex, bufferIndex);

        // Map each UUID associated with the table to the table's index
        for (Long tableUuid : uuids) {
            _fileIndexByUuid.put(tableUuid, index);
        }

        return index;
    }

    /**
     * Reads the table located at the given index.
     */
    private Table readTable(int index) {
        // Get the block
        TableBlock block = _blocks.get(getBlock(index));
        // Return the table located at the block offset
        return block.getTable(getBlockOffset(index));
    }

    @Override
    public void close()
            throws IOException {
        for (TableBlock block : _blocks) {
            Closeables.close(block, true);
        }
    }

    private int getBlock(int index) {
        return index / _blockSize;
    }

    private int getBlockOffset(int index) {
        return index % _blockSize;
    }

    private int toIndex(int buffer, int bufferIndex) {
        return buffer * _blockSize + bufferIndex;
    }

    /**
     * Checks whether the given block has a buffer assigned to it.  If not, it synchronously gets an available buffer
     * and assigns it to the block.
     */
    private void ensureBufferAvailable(TableBlock block) throws IOException {
        if (!block.hasBuffer()) {
            synchronized (this) {
                if (!block.hasBuffer()) {
                    ByteBuffer buffer = getBuffer();
                    block.setBuffer(buffer);
                }
            }
        }
    }

    /**
     * Gets a buffer for assigning to a TableBlock.  If less than {@link #_bufferCount} buffers have been created
     * then it will create a new one, otherwise it will take the buffer from the least recently used TableBlock.
     */
    private ByteBuffer getBuffer() throws IOException {
        ByteBuffer buffer;

        // Get the oldest block that has a buffer
        int bufferCount = 0;
        TableBlock takeFromBlock = null;
        for (TableBlock block : _blocks) {
            if (block.hasBuffer()) {
                bufferCount++;
                if (takeFromBlock == null || block.getMostRecentUse() < takeFromBlock.getMostRecentUse()) {
                    takeFromBlock = block;
                }
            }
        }
        if (bufferCount < _bufferCount || takeFromBlock == null) {
            // We haven't used the maximum number of buffers yet, so allocate a new one
            _log.debug("Allocating new block of size {}", _blockSize);
            buffer = ByteBuffer.allocate(_blockSize);
        } else {
            buffer = takeFromBlock.flushAndReleaseBuffer();
            // Reset the buffer for reuse
            buffer.position(0);
            buffer.limit(_blockSize);
        }

        return buffer;
    }

    /**
     * Class which maintains a block of the table cache.  The instance itself will always remain in memory, although
     * the cache buffer itself may be flushed to disk and reloaded as space and use requires.
     */
    private class TableBlock implements Closeable {
        private static final int UNKNOWN = -1;
        private static final int DROPPED = -2;

        // Logical index of the first bye in this block
        private final int _startIndex;
        private final ReentrantLock _lock = new ReentrantLock();
        private final Condition _flushReady = _lock.newCondition();
        private final Condition _flushComplete = _lock.newCondition();

        // File where the contents will be flushed to (if necessary)
        private Path _backingFile;
        // Last time this block was read from or written to; used in MRU computation for flushing caches
        private long _mostRecentUse;
        // Buffer which contains the block contents; may be null if the cache is flushed and unloaded
        private volatile ByteBuffer _buffer;
        // Boolean flag for whether the block is loaded
        private volatile boolean _loaded = false;
        // Boolean flag for whether the block has been modified
        private volatile boolean _modified = false;
        // Boolean flag for whether there is a flush request pending (flush must wait until there are no readers or writers on the block)
        private volatile boolean _flushPending;
        // Number of threads currently reading from or writing to the block
        private volatile int _bufferUseCount;

        private TableBlock(int startIndex) {
            _startIndex = startIndex;
        }

        /**
         * Writes a table to the block.  Returns the offset in this block where the table was written, or -1 if there
         * was insufficient room in the block to write the entire table.
         */
        public Pair<Integer, Set<Long>> writeTable(long uuid) {
            preBufferAccess();
            int offset = _buffer.position();
            try {
                // Leave 4 bytes for the length
                if (_buffer.remaining() < 4) {
                    // This buffer doesn't even have 4 bytes for the length
                    return Pair.<Integer, Set<Long>>create(-1, ImmutableSet.<Long>of());
                }
                _buffer.position(offset + 4);

                Set<Long> uuids;

                try {
                    ByteBuffer tableBuffer = _buffer.slice();
                    uuids = getTableSerializer().loadAndSerialize(uuid, new ByteBufferOutputStream(tableBuffer));
                    // Get the length of the bytes written
                    tableBuffer.flip();
                    int length = tableBuffer.limit();

                    // Write the length of the table's bytes
                    _buffer.position(offset);
                    _buffer.putInt(length);
                    // Move to the first unused byte in the buffer
                    _buffer.position(offset + 4 + length);
                } catch (UnknownTableException | DroppedTableException e) {
                    uuids = ImmutableSet.of(uuid);
                    writeUnknownOrDroppedTable(offset, e);
                }

                // Mark that the block has been modified
                _modified = true;

                return Pair.create(offset, uuids);
            } catch (BufferOverflowException e) {
                // Wasn't enough room in the buffer to write the table
                _buffer.position(offset);

                // If we started at the beginning of the buffer then the content itself is too large to fit in any block
                if (offset == 0) {
                    _log.error("Table with UUID {} is too large to fit in a single block", uuid);
                    throw new IllegalArgumentException("Table too large");
                }

                return Pair.<Integer, Set<Long>>create(-1, ImmutableSet.<Long>of());
            } catch (IOException e) {
                throw Throwables.propagate(e);
            } finally {
                postBufferAccess();
            }
        }

        /**
         * Gets the table that starts at the given offset.
         */
        public Table getTable(int offset)
                throws UnknownTableException, DroppedTableException {
            preBufferAccess();
            try {
                // Duplicate the buffer to allow for concurrent reads without locking
                ByteBuffer dup = _buffer.asReadOnlyBuffer();
                // Move to the given offset
                dup.position(offset);
                // Read the length
                int length = dup.getInt();
                if (length < 0) {
                    // This was an exception
                    throwUnknownOrDroppedTableException(dup, length);
                }

                // Restrict the buffer to only read the length of the table
                dup = (ByteBuffer) dup.slice().limit(length);
                try (InputStream in = new ByteBufferInputStream(dup)) {
                    return getTableSerializer().deserialize(in);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            } finally {
                postBufferAccess();
            }
        }

        /**
         * Called prior to reading from or writing to the block.  Ensures that all of the following are satisfied:
         * 1.  Block access will not be attempted while a flush is pending or taking place
         * 2.  Prevents a flush from starting until the block access is complete
         * 3.  Ensures that the block is loaded and ready for access
         */
        private void preBufferAccess() {
            // Mark the current time as the most recent use of this block
            _mostRecentUse = System.currentTimeMillis();

            _lock.lock();
            try {
                // If a flush is pending or active wait for it to complete
                while (_flushPending) {
                    _flushComplete.await();
                }
                // Make sure the buffer is available and loaded
                ensureBufferAvailable(this);
                ensureLoaded();
                // Increment the buffer use count to prevent a flush until the access is complete
                _bufferUseCount++;
            } catch (Exception e) {
                throw Throwables.propagate(e);
            } finally {
                _lock.unlock();
            }
        }

        /**
         * Called after a block is read from or written to.
         */
        private void postBufferAccess() {
            _lock.lock();
            try {
                // Decrement the buffer use count.  If this thread was the last accessor notify pending flushes that
                // they can proceed (if any)
                if (--_bufferUseCount == 0) {
                    _flushReady.signalAll();
                }
            } finally {
                _lock.unlock();
            }
        }

        /**
         * Records the exception when a table was dropped or unknown so an equivalent exception can be rethrown each
         * time the cached table is read.
         */
        private void writeUnknownOrDroppedTable(int offset, Exception e) {
            byte[] exceptionBytes = JsonHelper.asJson(e).getBytes(Charsets.UTF_8);
            // Reset the position to the start of this table
            _buffer.position(offset);
            // Mark the unknown or dropped flag in the "length" field
            _buffer.putInt(e instanceof UnknownTableException ? UNKNOWN : DROPPED);
            // Write the actual length of the exception JSON
            _buffer.putInt(exceptionBytes.length);
            _buffer.put(exceptionBytes);
        }

        /**
         * Reads and throws an exception previously written by {@link #writeUnknownOrDroppedTable(int, Exception)}.
         * The buffer should already be placed at the first byte after the exception int was read.
         */
        private void throwUnknownOrDroppedTableException(ByteBuffer buffer, int exceptionType)
                throws UnknownTableException, DroppedTableException {
            int length = buffer.getInt();
            String json = Charsets.UTF_8.decode((ByteBuffer) buffer.slice().limit(length)).toString();
            if (exceptionType == UNKNOWN) {
                throw JsonHelper.fromJson(json, UnknownTableException.class);
            } else {
                throw JsonHelper.fromJson(json, DroppedTableException.class);
            }
        }

        /**
         * Flushes the block's buffer to disk and releases the buffer for use by another block.  This method is called
         * when another block requires a buffer, all buffers have been allocated and this block is the least
         * recently used.
         */
        public ByteBuffer flushAndReleaseBuffer()
                throws IOException {
            _lock.lock();
            try {
                // Mark that a flush is pending to prevent any asynchronous reads or writes from starting
                _flushPending = true;

                // Wait for any current reads or writes to complete
                while (_bufferUseCount != 0) {
                    _flushReady.await();
                }

                // Flip the buffer to reset the position and limit for flushing
                _buffer.flip();

                // We only actually need to flush to disk if the buffer is non-empty or has changed since the last flush
                if (_buffer.limit() != 0 && _modified) {
                    if (_backingFile == null) {
                        // First flush; create a temporary file
                        _backingFile = Files.createTempFile("tablebuffer", ".tmp");
                    }
                    _log.debug("Flushing buffer for index {} to {} ({}/{} bytes)", _startIndex, _backingFile, _buffer.limit(), _blockSize);
                    try (ByteChannel out = Files.newByteChannel(_backingFile, WRITE)) {
                        out.write(_buffer);
                    }
                } else {
                    _log.debug("Releasing unmodified buffer for index {}", _startIndex);
                }

                ByteBuffer oldBuffer = _buffer;

                // Reset the buffer and loaded flags
                _buffer = null;
                _loaded = false;

                return oldBuffer;
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            } finally {
                // Mark the flush as complete and notify and reads or writes that were blocked
                _flushPending = false;
                _flushComplete.signalAll();
                _lock.unlock();
            }
        }

        /**
         * Sets the buffer for this block.  Once set the instance still has to call {@link #ensureLoaded()}
         * to load any data previously flushed.
         */
        public void setBuffer(ByteBuffer buffer) {
            checkState(_buffer == null, "setBuffer() called while a buffer is already assigned");
            _buffer = buffer;
            _modified = false;
        }

        /**
         * Loads the data from a previous flush into the buffer if it hasn't been loaded already.
         */
        private void ensureLoaded()
                throws IOException {
            if (!_loaded) {
                if (_backingFile != null) {
                    _log.debug("Loading buffer for index {} from {}", _startIndex, _backingFile);
                    try (ByteChannel in = Files.newByteChannel(_backingFile, READ)) {
                        in.read(_buffer);
                    }
                }
                _loaded = true;
            }
        }

        public boolean hasBuffer() {
            return _buffer != null;
        }

        public  long getMostRecentUse() {
            return _mostRecentUse;
        }

        @Override
        public void close()
                throws IOException {
            if (_backingFile != null) {
                _log.debug("Deleting file index {} from {}", _startIndex, _backingFile);
                Files.delete(_backingFile);
            }
        }
    }
}
