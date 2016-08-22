package com.bazaarvoice.emodb.web.report;

import com.bazaarvoice.emodb.sor.api.report.LongStatistics;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;

import static com.google.common.base.Preconditions.checkNotNull;

public class LongReportHistogram extends ReportHistogram<Long> {

    private final static ReportValueConverter<Long> _valueConverter = new ReportValueConverter<Long>() {
        @Override
        public double toDouble(Long value) {
            return value.doubleValue();
        }

        @Override
        public Long fromDouble(double value) {
            return (long) value;
        }
    };

    private final static Generator<Long, LongReportHistogram> _generator = new Generator<Long, LongReportHistogram>() {
        @Override
        public LongReportHistogram generateFrom(ReportSample<Long> sample, Long min, Long max, BigDecimal sum, long count, double m, double s) {
            return new LongReportHistogram(sample, min, max, sum, count, m, s);
        }
    };

    public LongReportHistogram() {
        super(_valueConverter);
    }

    @JsonCreator
    private LongReportHistogram(@JsonProperty ("sample") ReportSample<Long> sample, @JsonProperty ("min") Long min,
                                @JsonProperty ("max") Long max,  @JsonProperty ("sum") BigDecimal sum,
                                @JsonProperty ("count") long count, @JsonProperty ("m") double m,
                                @JsonProperty ("s") double s) {
        super(sample, min, max, sum, count, m, s, _valueConverter);
    }

    public LongReportHistogram combinedWith(LongReportHistogram other) {
        checkNotNull(other);
        return combine(this, other);
    }

    public static LongReportHistogram combine(LongReportHistogram h1, LongReportHistogram h2) {
        checkNotNull(h1);
        checkNotNull(h2);
        return combine(ImmutableList.of(h1, h2));
    }

    public static LongReportHistogram combine(Iterable<LongReportHistogram> histograms) {
        checkNotNull(histograms);
        return ReportHistogram.combine(histograms, _generator);
    }

    public LongStatistics toStatistics() {
        if (count() == 0) {
            return new LongStatistics();
        }
        return new LongStatistics(sum().longValue(), min(), max(), _valueConverter.fromDouble(mean()), stdDev(), sample());
    }
}
