package com.bazaarvoice.emodb.sor.api.report;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Math.floor;

abstract public class Statistics<T extends Comparable<T>> {
    private final T _min;
    private final T _max;
    private final T _mean;
    private final double _stdev;
    private final List<T> _sample;

    protected Statistics(T min, T max, T mean, double stdev, List<T> sample) {
        if (min == null) {
            if (max != null || mean != null || (sample != null && !sample.isEmpty())) {
                throw new IllegalArgumentException("Empty statistics must contain all null or empty values");
            }
        } else {
            if (max == null || mean == null || sample == null || sample.isEmpty()) {
                throw new IllegalArgumentException("Non-empty statistics must be fully initialized");
            }
            if (min.compareTo(max) > 0) {
                throw new IllegalArgumentException("Min cannot be greater than max");
            }
            if (min.compareTo(mean) > 0) {
                throw new IllegalArgumentException("Min cannot be greater than mean");
            }
            if (mean.compareTo(max) > 0) {
                throw new IllegalArgumentException("Mean cannot be greater than max");
            }
        }

        _min = min;
        _max = max;
        _mean = mean;
        _stdev = stdev;

        // The sample must be sorted.
        if (sample == null) {
            _sample = Collections.emptyList();
        } else {
            _sample = Collections.unmodifiableList(sample.stream().sorted().collect(Collectors.toList()));
        }
    }

    public T getMin() {
        return _min;
    }

    public T getMax() {
        return _max;
    }

    public T getMean() {
        return _mean;
    }

    public double getStdev() {
        return _stdev;
    }

    public List<T> getSample() {
        return _sample;
    }

    public T getPercentile(double percentile) {
        if (percentile < 0.0 || percentile > 1.0) {
            throw new IllegalArgumentException(percentile + " is not in [0..1]");
        }

        if (_sample.isEmpty()) {
            return null;
        }

        final double pos = percentile * (_sample.size() + 1);

        if (pos < 1) {
            return _sample.get(0);
        }

        if (pos >= _sample.size()) {
            return _sample.get(_sample.size() - 1);
        }

        final double lower = toDouble(_sample.get((int) pos - 1));
        final double upper = toDouble(_sample.get((int) pos));
        return fromDouble(lower + (pos - floor(pos)) * (upper - lower));
    }

    public T getMedian() {
        return getPercentile(0.5);
    }

    public T get95thPercentile() {
        return getPercentile(0.95);
    }

    public T get99thPercentile() {
        return getPercentile(0.99);
    }

    abstract protected double toDouble(T value);

    abstract protected T fromDouble(double value);
}
