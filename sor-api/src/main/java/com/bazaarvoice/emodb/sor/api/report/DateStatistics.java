package com.bazaarvoice.emodb.sor.api.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.Date;
import java.util.List;

public class DateStatistics extends Statistics<Date> {

    public DateStatistics() {
        this(null, null, null, 0, ImmutableList.<Date>of());
    }

    @JsonCreator
    public DateStatistics(
            @JsonProperty ("min") Date min, @JsonProperty ("max") Date max,
            @JsonProperty ("mean") Date mean, @JsonProperty ("stdev") double stdev,
            @JsonProperty ("sample") List<Date> sample) {
        super(min, max, mean, stdev, sample);
    }

    @Override
    protected double toDouble(Date value) {
        return value.getTime();
    }

    @Override
    protected Date fromDouble(double value) {
        return new Date((long) value);
    }
}
