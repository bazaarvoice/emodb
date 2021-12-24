package com.bazaarvoice.emodb.sortedq.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.math.LongMath;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

class Segment {
    private static final Logger _log = LoggerFactory.getLogger(Segment.class);

    /**
     * Bump this version number when making backward-incompatible changes to the segment snapshot format.
     */
    private static final int PERSISTENCE_VERSION = 1;

    /**
     * Configure the HyperLogLog cardinality estimator with a relatively small size to reduce the # of bytes required
     * to store the estimator and make it cheap to write to disk frequently, eg. on every read or write batch.  The
     * sorted queue algorithm tolerates imperfect estimates without trouble.
     * <p>
     * A HyperLogLog log2m value of 9 uses 352 bytes and has a relative standard deviation of 4.60%.
     * A HyperLogLog log2m value of 10 uses 692 bytes and has a relative standard deviation of 3.25%.
     * A HyperLogLog log2m value of 11 uses 1376 bytes and has a relative standard deviation of 2.30%.
     * #bytes formula: floor((2^log2m)/6)*4+8.  Rsd formula: 1.04 / sqrt(2^log2m)
     */
    @VisibleForTesting
    static final int HLL_LOG2M = 9;

    private final UUID _id;
    private final UUID _dataId;
    private boolean _deleted;
    private ByteBuffer _min;
    private int _adds;
    private long _bytesAdded;
    private final ICardinality _distinctAdds;
    private int _deletes;
    private final long _bytesUntilSplitCheckSize;
    private long _bytesUntilSplitCheckRemaining;
    private final SplitQueue<Segment> _splitQueue;
    private final long _splitThresholdBytes;
    private boolean _splitting;
    private long _splitTargetSize;
    private long _splitTargetRemaining;

    /**
     * Creates a brand new segment.
     */
    Segment(UUID id, @Nullable ByteBuffer min, long splitThresholdBytes, SplitQueue<Segment> splitQueue) {
        _id = id;
        _dataId = TimeUUIDs.newUUID();
        _min = min;
        _distinctAdds = new HyperLogLog(HLL_LOG2M);
        _bytesUntilSplitCheckSize = splitThresholdBytes / 16;
        _bytesUntilSplitCheckRemaining = _bytesUntilSplitCheckSize;
        _splitThresholdBytes = splitThresholdBytes;
        _splitQueue = requireNonNull(splitQueue, "splitQueue");
    }

    /**
     * Loads a segment from a persistent snapshot.
     */
    Segment(UUID id, Snapshot snapshot, long splitThresholdBytes, SplitQueue<Segment> splitQueue) {
        // Fail if the segment was written with a newer incompatible data format.
        if (snapshot.version > PERSISTENCE_VERSION) {
            throw new UnsupportedOperationException("Unsupported persistent sorted queue data version: " + snapshot.version);
        }
        _id = requireNonNull(id, "id");
        _dataId = Optional.ofNullable(snapshot.dataId).orElse(id);  // dataId should be non-null except for segments before dataId was introduced
        _min = (snapshot.min != null) ? ByteBufferUtil.hexToBytes(snapshot.min) : null;
        _adds = snapshot.adds;
        _bytesAdded = snapshot.bytesAdded;
        try {
            _distinctAdds = HyperLogLog.Builder.build(requireNonNull(snapshot.distinctAddsHll, "distinctAddsHll"));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        _deletes = snapshot.deletes;
        _bytesUntilSplitCheckSize = snapshot.bytesUntilSplitCheckSize;
        _bytesUntilSplitCheckRemaining = 0;
        _splitThresholdBytes = splitThresholdBytes;
        _splitting = snapshot.splitting;
        _splitTargetSize = snapshot.splitTargetSize;
        _splitTargetRemaining = snapshot.splitTargetRemaining;
        _splitQueue = requireNonNull(splitQueue, "splitQueue");
    }

    Snapshot snapshot() {
        try {
            Snapshot snapshot = new Snapshot();
            snapshot.dataId = _dataId;
            snapshot.min = (_min != null) ? ByteBufferUtil.bytesToHex(_min) : null;
            snapshot.adds = _adds;
            snapshot.bytesAdded = _bytesAdded;
            snapshot.distinctAddsHll = _distinctAdds.getBytes();
            snapshot.deletes = _deletes;
            snapshot.bytesUntilSplitCheckSize = _bytesUntilSplitCheckSize;
            snapshot.splitting = _splitting;
            snapshot.splitTargetSize = _splitTargetSize;
            snapshot.splitTargetRemaining = _splitTargetRemaining;
            return snapshot;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    UUID getId() {
        return _id;
    }

    UUID getDataId() {
        return _dataId;
    }

    boolean isDeleted() {
        return _deleted;
    }

    ByteBuffer getMin() {
        return _min;
    }

    void setMin(ByteBuffer min) {
        _min = min;
    }

    void onSegmentDeleted() {
        _deleted = true;
        _splitQueue.remove(this);
    }

    void onRecordAdded(ByteBuffer record) {
        _adds++;
        _bytesAdded += record.remaining();
        _distinctAdds.offerHashed(hash(record).asLong());

        // If the number of bytes ever written to the throw crosses a threshold, start slowing splitting it
        // up across multiple new segments to prevent Cassandra rows from getting too large.  Check whether
        // the threshold has been crossed only occasionally since estimating cardinality is relatively slow--
        // it involves floating point math, Math.log, etc.
        if (!_splitting && (_bytesUntilSplitCheckRemaining -= record.remaining()) <= 0) {
            long estimatedSizeBytes = distinctAdds() * recordSize();
            if (estimatedSizeBytes < _splitThresholdBytes) {
                _bytesUntilSplitCheckRemaining += _bytesUntilSplitCheckSize;
            } else {
                _log.debug("Splitting segment {} at {} bytes.", _id, estimatedSizeBytes);
                startSplitting();
            }
        }
    }

    void onRecordsDeleted(int count) {
        _deletes += count;
    }

    private void startSplitting() {
        _splitting = true;
        _splitTargetSize = Math.round(segmentSize() * 1.1 / 4);  // Split into 4 segments.  Assume segmentSize() is up to 10% lower than actual.
        _splitTargetRemaining = 0;
        _splitQueue.offer(this);
    }

    boolean isSplitting() {
        return _splitting;
    }

    /**
     * Tracks the # of bytes that have been moved.  Returns true if the splitter should create a new destination segment.
     */
    boolean onSplitWork(int bytesMoved) {
        if (_splitTargetRemaining <= 0) {
            _splitTargetRemaining = _splitTargetSize - bytesMoved;
            return true;
        } else {
            _splitTargetRemaining -= bytesMoved;
            return false;
        }
    }

    /**
     * Returns the estimated # of live records in this segment.
     */
    int cardinality() {
        return Math.max(distinctAdds() - _deletes, 1);
    }

    /**
     * Returns the estimated # of distinct records ever written to this segment.
     */
    int distinctAdds() {
        return (int) Math.min(_adds, _distinctAdds.cardinality());
    }

    /**
     * Returns the estimated # of bytes in this segment.
     */
    long segmentSize() {
        return cardinality() * recordSize();
    }

    /**
     * Returns the estimated # of bytes per record.
     */
    long recordSize() {
        return Math.max(LongMath.divide(_bytesAdded, Math.max(_adds, 1), RoundingMode.CEILING), 1);
    }

    private HashCode hash(ByteBuffer buf) {
        HashFunction hashFn = Hashing.murmur3_128();
        if (buf.hasArray()) {
            return hashFn.hashBytes(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        } else {
            return hashFn.hashBytes(ByteBufferUtil.getArray(buf));
        }
    }

    /**
     * For debugging.
     */
    @Override
    public String toString() {
        return String.format("Segment[%s%smin=%s;size=%,d;bytes=%,d;added=%,d;deleted=%,d;id=%s;dataId=%s]",
                _deleted ? "DELETED;" : "", _splitting ? "SPLITTING;" : "",
                (_min != null) ? ByteBufferUtil.bytesToHex(_min) : null,
                cardinality(), segmentSize(), _adds, _deletes,
                _id.toString().substring(4, 8), // abbreviate for debugging, pick digits that vary most w/in a TimeUUID.
                _dataId.toString().substring(4, 8));
    }

    // This class is public for Jackson json serialization.
    public static class Snapshot {
        public int version = PERSISTENCE_VERSION;
        public UUID dataId;
        public String min;
        public int adds;
        public long bytesAdded;
        public byte[] distinctAddsHll;
        public int deletes;
        public long bytesUntilSplitCheckSize;
        public boolean splitting;
        public long splitTargetSize;
        public long splitTargetRemaining;
    }
}
