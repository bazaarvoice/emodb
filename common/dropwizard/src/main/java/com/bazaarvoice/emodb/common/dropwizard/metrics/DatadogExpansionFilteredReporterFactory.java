package com.bazaarvoice.emodb.common.dropwizard.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import io.dropwizard.metrics.BaseReporterFactory;
import io.dropwizard.metrics.DatadogReporterFactory;
import org.coursera.metrics.datadog.DatadogReporter;
import org.coursera.metrics.datadog.transport.AbstractTransportFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

/**
 * Implementation of {@link DatadogReporterFactory} with even more filtering capabilities.  Specifically, it allows filtering
 * expansions on metrics such as max, min and percentile.
 */
@JsonTypeName("datadogExpansionFiltered")
public class DatadogExpansionFilteredReporterFactory extends BaseReporterFactory {

    @JsonProperty("host")
    private String _host = null;
    @JsonProperty("tags")
    private List<String> _tags = null;
    @JsonProperty("includeExpansions")
    private List<String> _includeExpansions = null;
    @JsonProperty("excludeExpansions")
    private List<String> _excludeExpansions = null;

    @Valid
    @NotNull
    @JsonProperty("transport")
    private AbstractTransportFactory _transport = null;

    @VisibleForTesting
    public void setTransport(AbstractTransportFactory transport) {
        _transport = transport;
    }

    public ScheduledReporter build(MetricRegistry registry) {
        return DatadogReporter.forRegistry(registry)
                .withTransport(_transport.build())
                .withHost(_host)
                .withTags(_tags)
                .filter(getFilter())
                .withExpansions(getExpansions())
                .convertDurationsTo(getDurationUnit())
                .convertRatesTo(getRateUnit())
                .build();
    }

    private EnumSet<DatadogReporter.Expansion> getExpansions() {
        final EnumSet<DatadogReporter.Expansion> expansions;

        if (_includeExpansions == null) {
            expansions = EnumSet.allOf(DatadogReporter.Expansion.class);
        } else {
            expansions = EnumSet.copyOf(asExpansions(_includeExpansions));
        }

        if (_excludeExpansions != null) {
            expansions.removeAll(asExpansions(_excludeExpansions));
        }

        return expansions;
    }

    private Collection<DatadogReporter.Expansion> asExpansions(Collection<String> strings) {
        return Collections2.transform(strings, new Function<String, DatadogReporter.Expansion>() {
            @Override
            public DatadogReporter.Expansion apply(String string) {
                for (DatadogReporter.Expansion expansion : DatadogReporter.Expansion.values()) {
                    if (expansion.toString().equals(string)) {
                        return expansion;
                    }
                }
                throw new IllegalArgumentException("Unknown expansion: " + string);
            }
        });
    }
}
