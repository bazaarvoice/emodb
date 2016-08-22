package com.bazaarvoice.emodb.hadoop.io;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.hadoop.json.IntrinsicsOnly;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.google.common.base.Charsets;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterables;
import org.apache.hadoop.hdfs.util.ByteBufferOutputStream;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;

/**
 * Hadoop representation of an EmoDB row.
 */
public class Row extends BytesWritable {

    // Map representation of the content.  Null if it hasn't been resolved
    private Map<String, Object> _map;
    // UTF-8 representation of the content.  Limit == 0 when the content has not been resolved.
    private ByteBuffer _text = ByteBuffer.allocate(0);

    public Row() {
        // empty
    }

    /** Generates a row from a JSON string. */
    public Row(String json) {
        set(json);
    }

    /** Generates a row from a UTF-8 representation of a JSON string.  The entire array is read. */
    public Row(byte[] utf8Bytes) {
        set(utf8Bytes);
    }

    /** Generates a row from a UTF-8 representation of a JSON string.  Only the first "length" bytes of the array are read. */
    public Row(byte[] bytes, int length) {
        set(bytes, 0, length);
    }

    /** Generates a row from a map representation of the content. */
    public Row(Map<String, Object> map) {
        set(map);
    }

    public void set(String json) {
        set(json.getBytes(Charsets.UTF_8));
    }

    @Override
    public void set(BytesWritable other) {
        if (other instanceof Row) {
            set(other.getBytes(), 0, other.getLength());
        } else {
            throw new IllegalArgumentException("Cannot set content from " + other.getClass());
        }
    }

    public void set(byte[] utf8) {
        set(utf8, 0, utf8.length);
    }

    @Override
    public void set(byte[] utf8, int start, int len) {
        // Reuse the buffer unless does not have sufficient capacity.
        if (len > _text.capacity()) {
            _text = ByteBuffer.allocate(len);
        }
        // Read the bytes into the buffer
        _text.clear();
        _text.put(utf8, start, len);
        // Flip the buffer to set the limit and move the position back to zero
        _text.flip();
        // Set the map to null, forcing it to be lazily loaded if getMap() is eventually called.
        _map = null;
    }

    public void set(Map<String, Object> map) {
        _map = map;
        // Reset the text length to zero, forcing it to be lazily loaded if the text is eventually needed.
        _text.position(0);
        _text.limit(0);
    }

    /**
     * Ensures that either the UTF-8 text has been set directly or by indirectly converting the Map contents to JSON.
     */
    private void ensureTextSet() {
        if (_text.limit() == 0) {
            checkState(_map != null, "Neither JSON text nor map has been set");
            _text.clear();
            // First try reading the JSON directly into be buffer.
            try {
                JsonHelper.writeJson(new ByteBufferOutputStream(_text), _map);
                // Set the limit and move the position back to zero.
                _text.flip();
            } catch (Exception e) {
                if (Iterables.tryFind(Throwables.getCausalChain(e), Predicates.instanceOf(BufferOverflowException.class)).isPresent()) {
                    // Buffer was insufficient.  Allocate a new array and read the bytes into it.
                    byte[] utf8 = JsonHelper.asUtf8Bytes(_map);
                    _text = ByteBuffer.wrap(utf8);
                } else {
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    /**
     * Gets an EmoDB Map<String, Object> representation of the row's contents.
     * Note:  For performance reasons the original map is returned to the caller, not a copy, and therefore mutability
     *        is not guaranteed.  However, the caller should NOT modify the Map's contents since it is
     *        not deterministic whether those changes will be present when the Row is serialized.  To update this
     *        instance the caller should generate a new map and update the contents by calling {@link #set(java.util.Map)}.
     */
    public Map<String, Object> getMap() {
        if (_map == null) {
            checkState(_text.limit() != 0, "Neither JSON text nor map has been set");
            //noinspection unchecked
            _map = JsonHelper.fromUtf8Bytes(getBytes(), 0, getLength(), Map.class);
        }
        return _map;
    }

    public String getJson() {
        ensureTextSet();
        return new String(_text.array(), 0, _text.limit(), Charsets.UTF_8);
    }

    public String getTable() {
        return Intrinsic.getTable(getMap());
    }

    public String getId() {
        return Intrinsic.getId(getMap());
    }

    public long getVersion() {
        return Intrinsic.getVersion(getMap());
    }

    public String getSignature() {
        return Intrinsic.getSignature(getMap());
    }

    public Date getFirstUpdateAt() {
        return Intrinsic.getFirstUpdateAt(getMap());
    }

    public Date getLastUpdateAt() {
        return Intrinsic.getLastUpdateAt(getMap());
    }

    /**
     * Override the parent to compare the Map values since order of the fields in a JSON string is irrelevant.
     */
    @Override
    public boolean equals(Object o) {
        return this == o || o != null && o instanceof Row && Objects.equals(getMap(), ((Row) o).getMap());
    }

    @Override
    public int compareTo(BinaryComparable o) {
        if (this == o) {
            return 0;
        }
        if (!(o instanceof Row)) {
            throw new IllegalArgumentException("Cannot compare row to " + o.getClass());
        }
        Row other = (Row) o;
        return ComparisonChain.start()
                .compare(getTable(), other.getTable())
                .compare(getId(), other.getId())
                .compare(getVersion(), other.getVersion())
                .result();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getTable());
    }

    /**
     * Sets this instance's content from the input.
     */
    @Override
    public void readFields(DataInput in)
            throws IOException {
        // Read the length as a variable int
        int length = WritableUtils.readVInt(in);
        // If necessary increase the buffer capacity
        if (length > _text.capacity()) {
            _text = ByteBuffer.allocate(length);
        }
        // For efficiency read directly into the buffer's array
        in.readFully(_text.array(), 0, length);
        // Since we bypassed putting into the buffer set the position and limit directly
        _text.position(0);
        _text.limit(length);
        // Set the map to null since the contents may have changed.
        _map = null;
    }

    /**
     * Writes this instance's content to the output.  Note that the format for Row is identical to Text, so even though
     * the classes are unrelated a Text object can read the bytes written by a Row as a JSON string.
     */
    @Override
    public void write(DataOutput out)
            throws IOException {
        ensureTextSet();
        // Write a variable int with the length
        WritableUtils.writeVInt(out, _text.limit());
        // Write the bytes.  For efficiency directly access the buffer's array.
        out.write(_text.array(), 0, _text.limit());
    }

    @Override
    public byte[] copyBytes() {
        ensureTextSet();
        byte[] bytes = new byte[_text.limit()];
        _text.get(bytes);
        _text.position(0);
        return bytes;
    }

    @Override
    public byte[] getBytes() {
        ensureTextSet();
        return _text.array();
    }

    @Override
    public int getLength() {
        ensureTextSet();
        return _text.limit();
    }

    @Override
    public void setSize(int size) {
        _text.limit(size);
    }

    @Override
    public int getCapacity() {
        return _text.capacity();
    }

    @Override
    public void setCapacity(int newCapacity) {
        if (_text.capacity() != newCapacity) {
            // Allocate a buffer with the requested capacity
            ByteBuffer newText = ByteBuffer.allocate(newCapacity);
            // If the current buffer is larger decrease the limit to match the new capacity
            if (_text.limit() > newCapacity) {
                _text.limit(newCapacity);
                _map = null;
            }
            newText.put(_text);
            newText.flip();
            _text = newText;
        }
    }

    @Override
    public String toString() {
        return getJson();
    }

    /**
     * Register a comparator which works specifically with Rows.
     */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(Row.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            try {
                int n1 = WritableUtils.decodeVIntSize(b1[s1]);
                int n2 = WritableUtils.decodeVIntSize(b2[s2]);
                IntrinsicsOnly v1 = JsonHelper.fromUtf8Bytes(b1, s1 + n1, l1 - n1, IntrinsicsOnly.class);
                IntrinsicsOnly v2 = JsonHelper.fromUtf8Bytes(b1, s2 + n2, l2 - n2, IntrinsicsOnly.class);

                return ComparisonChain.start()
                        .compare(v1.table, v2.table)
                        .compare(v1.id, v2.id)
                        .compare(v1.version, v2.version)
                        .result();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final Comparator _comparatorInstance;

    static {
        // register this comparator
        WritableComparator.define(Row.class, _comparatorInstance = new Comparator());
    }

    /**
     * Override compareTo() to use the Row comparator.
     */
    @Override
    public int compareTo(byte[] other, int off, int len) {
        ensureTextSet();
        return _comparatorInstance.compare(
                getBytes(), 0, getLength(),
                other, off, len);
    }
}
