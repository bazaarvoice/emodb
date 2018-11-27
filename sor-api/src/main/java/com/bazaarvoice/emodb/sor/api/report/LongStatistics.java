package com.bazaarvoice.emodb.sor.api.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

public class LongStatistics extends Statistics<Long> {

    private final long _sum;

    public LongStatistics() {
        this(null, null, null, null, 0, Collections.emptyList());
    }

    @JsonCreator
    public LongStatistics(
            @JsonProperty ("sum") Long sum, @JsonProperty ("min") Long min, @JsonProperty ("max") Long max,
            @JsonProperty ("mean") Long mean, @JsonProperty ("stdev") double stdev,
            @JsonProperty ("sample") List<Long> sample) {
        super(min, max, mean, stdev, sample);
        if ((sum == null) != (min == null)) {
            throw new IllegalArgumentException("Inconsistent null value for sum");
        }
        _sum = sum;
    }

    public Long getSum() {
        return _sum;
    }

    @Override
    protected double toDouble(Long value) {
        return value.doubleValue();
    }

    @Override
    protected Long fromDouble(double value) {
        return (long) value;
    }
}
