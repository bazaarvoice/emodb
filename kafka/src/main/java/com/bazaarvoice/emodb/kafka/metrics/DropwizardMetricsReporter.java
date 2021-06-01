package com.bazaarvoice.emodb.kafka.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.coursera.metrics.datadog.TaggedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropwizardMetricsReporter implements MetricsReporter {

    private static Logger _log = LoggerFactory.getLogger(DropwizardMetricsReporter.class);

    private static final String REGISTRY_NAME = "default";
    private static final String METRIC_PREFIX = MetricsReporter.class.getPackage().getName();

    private MetricRegistry _registry;
    private Set<String> _metricNames = new HashSet<>();

    public static void registerDefaultMetricsRegistry(MetricRegistry metricRegistry) {
        SharedMetricRegistries.add(REGISTRY_NAME, metricRegistry);
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        _registry = SharedMetricRegistries.getOrCreate(REGISTRY_NAME);
        for (KafkaMetric kafkaMetric : metrics) {
            metricChange(kafkaMetric);
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        _log.debug("Processing a metric change for {}", metric.metricName());
        String name = metricName(metric);

        final Gauge<Object> gauge = metric::metricValue;


        _log.debug("Registering {}", name);
        try {
            _registry.register(name, gauge);
            _metricNames.add(name);
        } catch (IllegalArgumentException e) {
            _log.debug("metricChange called for `{}' which was already registered, ignoring.", name);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        String name = metricName(metric);
        _log.debug("Removing {}", name);
        _registry.remove(name);
        _metricNames.remove(name);
    }

    @Override
    public void close() {
        for (String name: _metricNames) {
            _registry.remove(name);
        }
    }

    private static String metricName(KafkaMetric kafkaMetric) {
        MetricName name = kafkaMetric.metricName();

        TaggedName.TaggedNameBuilder builder = new TaggedName.TaggedNameBuilder()
                .metricName(MetricRegistry.name(METRIC_PREFIX, name.group(), name.name()));

        name.tags().forEach(builder::addTag);

        return builder.build().encode();
    }

}
