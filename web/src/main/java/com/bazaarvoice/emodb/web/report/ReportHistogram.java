package com.bazaarvoice.emodb.web.report;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

/**
 * Custom implementation of a Histogram based on {@link com.codahale.metrics.Histogram} with the following
 * differences:
 *
 * <ul>
 *     <li>Each histogram is generated single threaded, so the thread safety overhead has been removed.</li>
 *     <li>This implementation can combine two existing Histograms into a new Histogram.</li>
 * </ul>
 */
abstract public class ReportHistogram<T extends Comparable<T>> {
    private static final int SAMPLE_SIZE = 1028;

    final private ReportValueConverter<T> _valueConverter;

    @JsonProperty ("sample")
    private final ReportSample<T> _sample;
    @JsonProperty ("min")
    private T _min;
    @JsonProperty ("max")
    private T _max;
    @JsonProperty ("sum")
    private BigDecimal _sum;
    @JsonProperty ("count")
    private long _count;
    @JsonProperty ("m")
    private double _m;
    @JsonProperty ("s")
    private double _s;

    protected ReportHistogram(ReportValueConverter<T> valueConverter) {
        _sample = new ReportSample<>(SAMPLE_SIZE);
        _valueConverter = valueConverter;
        clear();
    }

    protected ReportHistogram(ReportSample<T> sample, T min, T max, BigDecimal sum, long count, double m, double s,
                              ReportValueConverter<T> valueConverter) {
        _sample = sample;
        _min = min;
        _max = max;
        _sum = sum;
        _count = count;
        _m = m;
        _s = s;
        _valueConverter = valueConverter;
    }

    /**
     * Clears all recorded values.
     */
    public void clear() {
        _sample.clear();
        _count = 0;
        _max = null;
        _min = null;
        _sum = BigDecimal.ZERO;
        _m = -1;
        _s = 0;
    }

    public void update(T value) {
        double doubleValue = _valueConverter.toDouble(value);

        _count++;
        _sum = _sum.add(BigDecimal.valueOf(doubleValue));
        _sample.update(value);
        setMax(value);
        setMin(value);
        updateVariance(doubleValue);
    }

    public List<T> sample() {
        return _sample.values();
    }

    public long count() {
        return _count;
    }

    public T max() {
        return _max;
    }

    public T min() {
        return _min;
    }

    public double mean() {
        if (_count > 0) {
            try {
                return _sum.divide(BigDecimal.valueOf(_count), RoundingMode.HALF_UP).doubleValue();
            } catch (ArithmeticException e) {
                throw e;
            }
        }
        return 0.0;
    }

    public double stdDev() {
        if (_count > 0) {
            return sqrt(variance());
        }
        return 0.0;
    }

    public BigDecimal sum() {
        return _sum;
    }

    private double variance() {
        if (_count <= 1) {
            return 0.0;
        }
        return _s / (_count - 1);  // TODO: this is stdev of a sample, should be stdev of population.
    }

    private void setMax(T potentialMax) {
        _max = _max == null ? potentialMax : Ordering.natural().max(_max, potentialMax);
    }

    private void setMin(T potentialMin) {
        _min = _min == null ? potentialMin : Ordering.natural().min(_min, potentialMin);
    }

    private void updateVariance(double value) {
        double oldM = _m, oldS = _s;
        double newM, newS;

        if (oldM == -1) {
            newM = value;
            newS = 0;
        } else {
            newM = oldM + ((value - oldM) / count());
            newS = oldS + ((value - oldM) * (value - newM));
        }

        _m = newM;
        _s = newS;
    }

    protected static interface Generator<T extends Comparable<T>, H extends ReportHistogram<T>> {
        H generateFrom(ReportSample<T> sample, T min, T max, BigDecimal sum, long count, double m, double s);
    }

    protected static <T extends Comparable<T>, H extends ReportHistogram<T>> H combine(Iterable<H> histograms, Generator<T, H> generator) {
        checkNotNull(histograms);

        T min = null;
        T max = null;
        BigDecimal sum = BigDecimal.ZERO;
        long nc = 0;
        BigDecimal weightedSum = BigDecimal.ZERO;
        ReportSample<T> sample = new ReportSample<>(SAMPLE_SIZE);

        for (ReportHistogram<T> h : histograms) {
            checkNotNull(h);
            if (h._count != 0) {
                min = min == null ? h._min : Ordering.natural().min(min, h._min);
                max = max == null ? h._max : Ordering.natural().max(max, h._max);
                sum = sum.add(h._sum);
                nc += h._count;
                weightedSum = weightedSum.add(BigDecimal.valueOf(h._count * h.mean()));
                sample = sample.combinedWith(h._sample);
            }
        }

        double mc, sc;

        // TODO: This math isn't correct.  Merging histograms produces different results based on how the
        // TODO: values are grouped in the source histograms.
        if (nc == 0) {
            mc = -1;
            sc = 0;
        } else if (nc == 1) {
            mc = sum.doubleValue();
            sc = 0;
        } else {
            mc = weightedSum.divide(BigDecimal.valueOf(nc), RoundingMode.HALF_UP).doubleValue();
            double e = 0;
            for (ReportHistogram h : histograms) {
                e += h._count * (h.variance() + pow(h.mean() - mc, 2));
            }
            sc = e / nc * (nc-1);
        }

        try {
            return generator.generateFrom(sample, min, max, sum, nc, mc, sc);
        } catch (Exception e) {
            // Should never happen
            throw Throwables.propagate(e);
        }
    }
}