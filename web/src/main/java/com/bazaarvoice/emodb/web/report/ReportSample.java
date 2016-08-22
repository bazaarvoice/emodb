package com.bazaarvoice.emodb.web.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Sample class based on {@link com.yammer.metrics.stats.UniformSample} with the following differences:
 *
 * <ul>
 *     <li>Each sample is generated single threaded, so the locking overhead of UniformSample has been removed.</li>
 *     <li>This implementation can be initialized from a pre-existing set of data.</li>
 *     <li>This implementation allows for efficient combination of two separate samples.</li>
 * </ul>
 */
public class ReportSample<T extends Comparable<T>> {

    @JsonProperty ("values")
    private final List<T> _values;
    @JsonProperty ("capacity")
    private final int _capacity;
    @JsonProperty ("count")
    private long _count = 0;
    private final transient Random _random = new Random();

    public ReportSample(int size) {
        checkArgument(size >= 0, "Size must be non-negative");
        _values = Lists.newArrayListWithCapacity(size);
        _capacity = size;
    }

    @JsonCreator
    private ReportSample(@JsonProperty("capacity") int capacity,
                         @JsonProperty("count") long initCount,
                         @JsonProperty("values") List<T> initValues) {
        this(capacity, initCount, (Iterable<T>) initValues);
    }

    public ReportSample(int capacity, long initCount, Iterable<T> initValues) {
        this(capacity);

        checkArgument(initCount >= 0, "Initial count must be non-negative");
        checkNotNull(initValues, "initValues");

        int updateCount = update(initValues);

        checkArgument(updateCount == Math.min(capacity, initCount), "Invalid number of initial values");

        // Override whatever count was computed from inserting the initial values with the provided count
        _count = initCount;
    }

    public void clear() {
        _values.clear();
        _count = 0;
    }

    public int size() {
        return (int) Math.min(_count, _capacity);
    }

    public List<T> values() {
        return Ordering.natural().immutableSortedCopy(_values);
    }

    public void update(T value) {
        _count++;
        if (_count <= _capacity) {
            _values.add(value);
        } else {
            long rnd = Math.abs(_random.nextLong()) % _count;
            if (rnd < _capacity) {
                _values.set((int) rnd, value);
            }
        }
    }

    public int update(Iterable<T> values) {
        checkNotNull(values, "values");

        int updateCount = 0;
        for (T value : values) {
            update(value);
            updateCount++;
        }

        return updateCount;
    }

    public ReportSample<T> combinedWith(ReportSample<T> otherSample) {
        return combine(this, otherSample);
    }

    public static <T extends Comparable<T>> ReportSample<T> combine(ReportSample<T> s1, ReportSample<T> s2) {
        int capacity = Math.min(s1._capacity, s2._capacity);

        // If the combined sample contains more values than the capacity then proportionally take values from
        // each input based on their respective total sizes.

        long count1 = s1._count;
        long count2 = s2._count;
        long combinedCount = count1 + count2;

        if (combinedCount > capacity) {
            count1 = (int) (capacity * (float) count1 / (count1 + count2));
            count2 = capacity - count1;
        }

        Iterable<T> combinedValues = Iterables.concat(
                s1._values.subList(0, (int) count1),
                s2._values.subList(0, (int) count2)
        );

        return new ReportSample<>(capacity, combinedCount, combinedValues);
    }
}
