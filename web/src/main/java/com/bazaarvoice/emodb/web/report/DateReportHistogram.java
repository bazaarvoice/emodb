package com.bazaarvoice.emodb.web.report;

import com.bazaarvoice.emodb.sor.api.report.DateStatistics;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

public class DateReportHistogram extends ReportHistogram<Date> {

    private final static ReportValueConverter<Date> _valueConverter = new ReportValueConverter<Date>() {
        @Override
        public double toDouble(Date value) {
            return value.getTime();
        }

        @Override
        public Date fromDouble(double value) {
            return new Date((long) value);
        }
    };

    private final static Generator<Date, DateReportHistogram> _generator = new Generator<Date, DateReportHistogram>() {
        @Override
        public DateReportHistogram generateFrom(ReportSample<Date> sample, Date min, Date max, BigDecimal sum, long count, double m, double s) {
            return new DateReportHistogram(sample, min, max, sum, count, m, s);
        }
    };

    public DateReportHistogram() {
        super(_valueConverter);
    }

    @JsonCreator
    private DateReportHistogram(@JsonProperty ("sample") ReportSample<Date> sample, @JsonProperty ("min") Date min,
                                @JsonProperty ("max") Date max,  @JsonProperty ("sum") BigDecimal sum,
                                @JsonProperty ("count") long count, @JsonProperty ("m") double m,
                                @JsonProperty ("s") double s) {
        super(sample, min, max, sum, count, m, s, _valueConverter);
    }

    public Date dateMean() {
        if (count() == 0) {
            return null;
        }
        double mean = super.mean();
        return _valueConverter.fromDouble(mean);
    }

    public DateReportHistogram combinedWith(DateReportHistogram other) {
        checkNotNull(other);
        return combine(this, other);
    }

    public static DateReportHistogram combine(DateReportHistogram h1, DateReportHistogram h2) {
        checkNotNull(h1);
        checkNotNull(h2);
        return combine(ImmutableList.of(h1, h2));
    }

    public static DateReportHistogram combine(Iterable<DateReportHistogram> histograms) {
        checkNotNull(histograms);
        return ReportHistogram.combine(histograms, _generator);
    }

    public DateStatistics toStatistics() {
        if (count() == 0) {
            return new DateStatistics();
        }
        return new DateStatistics(min(), max(), dateMean(), stdDev(), sample());
    }
}
