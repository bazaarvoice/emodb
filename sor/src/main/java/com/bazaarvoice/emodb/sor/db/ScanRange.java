package com.bazaarvoice.emodb.sor.db;

import com.bazaarvoice.emodb.common.cassandra.nio.BufferUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedBytes;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.util.ByteBufferRangeImpl;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Defines an upper and lower bound for performing a scan of the underlying Cassandra table data column family.
 * Note that if {@link #getFrom()} is greater than {@link #getTo()}} then the scan will wrap from the high to low end
 * of the token range.
 */
public class ScanRange implements Comparable<ScanRange> {
    public final static ByteBuffer MIN_VALUE = ByteBuffer.wrap(new byte[0]);
    public final static ByteBuffer MAX_VALUE = generateMaxValue();

    private final ByteBuffer _from;
    private final ByteBuffer _to;

    @JsonCreator
    private ScanRange(@JsonProperty ("from") String from, @JsonProperty ("to") String to) {
        try {
            _from = ByteBuffer.wrap(Hex.decodeHex(checkNotNull(from, "from").toCharArray())).asReadOnlyBuffer();
            _to = ByteBuffer.wrap(Hex.decodeHex(checkNotNull(to, "to").toCharArray())).asReadOnlyBuffer();
        } catch (DecoderException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private ScanRange(ByteBuffer from, ByteBuffer to) {
        _from = checkNotNull(from, "from");
        _to = checkNotNull(to, "to");
    }

    public static ScanRange create(ByteBufferRange range) {
        return create(range.getStart(), range.getEnd());
    }

    public static ScanRange create(ByteBuffer from, ByteBuffer to) {
        return new ScanRange(from, to);
    }

    public static ScanRange all() {
        return create(MIN_VALUE, MAX_VALUE);
    }

    @JsonIgnore
    public ByteBuffer getFrom() {
        return _from;
    }

    @JsonIgnore
    public ByteBuffer getTo() {
        return _to;
    }

    @JsonProperty ("from")
    public String getFromHex() {
        return ByteBufferUtil.bytesToHex(_from);
    }

    @JsonProperty ("to")
    public String getToHex() {
        return ByteBufferUtil.bytesToHex(_to);
    }

    /**
     * If necessary splits the scan range into two range such that "from" is less than "to" in each range.
     * This is necessary if the scan range wraps the token range.
     */
    public List<ScanRange> unwrapped() {
        if (compare(_from, _to) < 0) {
            return ImmutableList.of(this);
        }
        ImmutableList.Builder<ScanRange> ranges = ImmutableList.builder();
        if (compare(_from, MAX_VALUE) < 0) {
            ranges.add(new ScanRange(_from, MAX_VALUE));
        }
        if (compare(_to, MIN_VALUE) > 0) {
            ranges.add(new ScanRange(MIN_VALUE, _to));
        }
        return ranges.build();
    }

    public ByteBufferRange asByteBufferRange() {
        return new ByteBufferRangeImpl(_from, _to, -1, false);
    }

    public List<ScanRange> intersection(ScanRange other) {
        return intersection(this, other);
    }

    /**
     * Returns the intersection subranges of two ScanRanges.  Normally an intersection of two linear spaces
     * would either return nothing or the single intersecting range.  However, since a ScanRange is circular it
     * is possible to provide two ScanRanges with two intersection ranges, as the following example shows.  This
     * is why this method returns a list of ScanRanges:
     *
     * Range 1:         XXXXXXX..............XXXXXXX
     * Range 2:         .....XXXXXXXXXXXXXXXXXX.....
     * Intersection:    .....XX..............XX.....
     *
     * @return A list of intersecting ranges, or empty if there was no intersection.
     */
    public static List<ScanRange> intersection(ScanRange left, ScanRange right) {
        List<ScanRange> unwrappedLeft = left.unwrapped();
        List<ScanRange> unwrappedRight = right.unwrapped();

        boolean leftWrapped = unwrappedLeft.size() > 1;
        boolean rightWrapped = unwrappedRight.size() > 1;

        if (!leftWrapped && !rightWrapped) {
            // Neither side wrapped the token range, so a simple intersection is all that is required.
            // Save some cycles by comparing the ranges directly.
            ScanRange intersection = intersectionUnwrapped(left, right);
            if (intersection == null) {
                return ImmutableList.of();
            }
            return ImmutableList.of(intersection);
        }

        List<ScanRange> intersections = Lists.newArrayListWithExpectedSize(2);
        for (ScanRange l : unwrappedLeft) {
            for (ScanRange r : unwrappedRight) {
                ScanRange intersection = intersectionUnwrapped(l, r);
                if (intersection != null) {
                    intersections.add(intersection);
                }
            }
        }

        return combine(intersections);
    }

    @Nullable
    private static ScanRange intersectionUnwrapped(ScanRange left, ScanRange right) {
        // Make sure left represents the range with the lower starting point
        if (compare(left._from, right._from) > 0) {
            ScanRange tmp = right;
            right = left;
            left = tmp;
        }

        if (compare(right._from, left._from) >= 0 && compare(right._from, left._to) < 0) {
            ByteBuffer endIntersection = compare(left._to, right._to) < 0 ? left._to : right._to;
            return ScanRange.create(right._from, endIntersection);
        }

        return null;
    }

    public List<ScanRange> union(ScanRange other) {
        return union(this, other);
    }

    /**
     * Returns the union of two ScanRanges.  If the ranges do not intersect then the union is each individual range.
     * Therefore this method will always return a list of one or two ranges, depending on whether there was a gap
     * between the token ranges.
     *
     * @return The minimal list of scan ranges representing the union of the two ranges.
     */
    public static List<ScanRange> union(ScanRange left, ScanRange right) {
        List<ScanRange> unwrappedLeft = left.unwrapped();
        List<ScanRange> unwrappedRight = right.unwrapped();

        boolean leftWrapped = unwrappedLeft.size() > 1;
        boolean rightWrapped = unwrappedRight.size() > 1;

        if (!leftWrapped && !rightWrapped) {
            // Neither side wrapped the token range, so a simple union is all that is required.
            // Save some cycles by comparing the ranges directly.
            ScanRange union = unionUnwrapped(left, right);
            if (union == null) {
                return ImmutableList.of(left, right);
            }
            return ImmutableList.of(union);
        }

        List<ScanRange> reducedLeft  = Lists.newArrayList(unwrappedLeft);
        List<ScanRange> reducedRight  = Lists.newArrayList(unwrappedRight);

        for (int l=0; l < reducedLeft.size(); l++) {
            for (int r=0; r < reducedRight.size(); r++) {
                ScanRange union = unionUnwrapped(reducedLeft.get(l), reducedRight.get(r));
                if (union != null) {
                    reducedLeft.set(l, union);
                    reducedRight.remove(r);
                    break;
                }
            }
        }

        if (reducedRight.isEmpty() && reducedLeft.size() == 1) {
            return reducedLeft;
        }

        List<ScanRange> union = Lists.newArrayListWithCapacity(reducedLeft.size() + reducedRight.size());
        union.addAll(reducedLeft);
        union.addAll(reducedRight);

        return combine(union);
    }

    @Nullable
    private static ScanRange unionUnwrapped(ScanRange left, ScanRange right) {
        if (left._from.equals(right._to) || right._from.equals(left._to) || intersectionUnwrapped(left, right) != null) {
            return new ScanRange(
                    compare(left._from, right._from) < 0 ? left._from : right._from,
                    compare(left._to, right._to) > 0 ? left._to : right._to);
        }

        return null;
    }

    /**
     * Combines multiple ranges resulting from an intersection or union operations.  All ranges are guaranteed possibly
     * be adjacent but never intersect.
     */
    private static List<ScanRange> combine(List<ScanRange> ranges) {
        if (ranges.size() > 1) {
            // For consistency always return the ranges sorted from low- to high-range.
            Collections.sort(ranges);

            // If multiple ranges are contiguous then join them.  This can happen if one of the ranges is "all".
            for (int i = ranges.size() - 1; i > 0; i--) {
                ScanRange union = unionUnwrapped(ranges.get(i-1), ranges.get(i));
                if (union != null) {
                    ranges.set(i-1, union);
                    ranges.remove(i);
                }
            }

            // If the ranges represent a contiguous high- to low-end wrapped range then combine them.
            int numRanges = ranges.size();
            if (numRanges >= 2 &&
                    ranges.get(0)._from.equals(MIN_VALUE) && ranges.get(numRanges-1)._to.equals(MAX_VALUE)) {
                ScanRange combined = ScanRange.create(ranges.get(numRanges-1)._from, ranges.get(0)._to);
                ranges.remove(numRanges-1);
                ranges.remove(0);
                ranges.add(combined);
            }
        }

        return ranges;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof ScanRange)) {
            return false;
        }

        ScanRange scanRange = (ScanRange) o;
        return scanRange.getFrom().equals(_from) && scanRange.getTo().equals(_to);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_from, _to);
    }

    @Override
    public int compareTo(ScanRange o) {
        // Sort by whichever has the lower "from" scan boundary
        return compare(_from, o._from);
    }

    @Override
    public String toString() {
        return String.format("ScanRange[%s-%s]", getFromHex(), getToHex());
    }

    private static ByteBuffer generateMaxValue() {
        // Shortcut to create the maximal row key, shard=0xff and tableUuid=0xffffffffffffffff
        byte[] bytes = new byte[9];
        Arrays.fill(bytes, UnsignedBytes.MAX_VALUE);
        return ByteBuffer.wrap(bytes);
    }

    private static int compare(ByteBuffer left, ByteBuffer right) {
        return BufferUtils.compareUnsigned(left, right);
    }
}
