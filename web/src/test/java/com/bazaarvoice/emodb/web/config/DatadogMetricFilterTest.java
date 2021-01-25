package com.bazaarvoice.emodb.web.config;

import com.bazaarvoice.emodb.common.dropwizard.metrics.DatadogExpansionFilteredReporterFactory;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.metrics.ReporterFactory;
import org.coursera.metrics.datadog.model.DatadogGauge;
import org.coursera.metrics.datadog.transport.AbstractTransportFactory;
import org.coursera.metrics.datadog.transport.Transport;
import org.mockito.ArgumentMatcher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

public class DatadogMetricFilterTest {

    private MetricRegistry _metricRegistry;
    private Transport.Request _request;

    @BeforeMethod
    public void setUp() {
        _metricRegistry = new MetricRegistry();
        _request = mock(Transport.Request.class);
    }

    @Test
    public void testExpansionFilterInclusion() throws Exception {
        String json =
                "{" +
                        "\"type\": \"datadogExpansionFiltered\"," +
                        "\"host\": \"test-host\"," +
                        "\"includeExpansions\": [\"count\", \"min\", \"max\", \"p95\"]," +
                        "\"transport\": {" +
                            "\"type\": \"http\"," +
                            "\"apiKey\": \"12345\"" +
                        "}" +
                        "}";


        ScheduledReporter reporter = createReporter(json);

        // Create some metrics for each major type
        Counter counter = _metricRegistry.counter("test.counter");
        counter.inc(10);

        Histogram histogram = _metricRegistry.histogram("test.histogram");
        histogram.update(1);
        histogram.update(2);
        histogram.update(3);

        Meter meter = _metricRegistry.meter("test.meter");
        meter.mark(100);

        Timer timer = _metricRegistry.timer("test.timer");
        timer.update(1, TimeUnit.SECONDS);
        timer.update(2, TimeUnit.SECONDS);
        timer.update(3, TimeUnit.SECONDS);

        _metricRegistry.register("test.gauge", (Gauge<Integer>) () -> 50);

        reporter.report();

        // Verify only the desired metrics were sent
        verify(_request).addGauge(argThat(hasGauge("test.counter", 10)));
        verify(_request).addGauge(argThat(hasGauge("test.histogram.count", 3)));
        verify(_request).addGauge(argThat(hasGauge("test.histogram.min", 1)));
        verify(_request).addGauge(argThat(hasGauge("test.histogram.max", 3)));
        verify(_request).addGauge(argThat(hasGauge("test.histogram.p95", 3.0)));
        verify(_request).addGauge(argThat(hasGauge("test.timer.count", 3)));
        verify(_request).addGauge(argThat(hasGauge("test.timer.min", 1000f)));
        verify(_request).addGauge(argThat(hasGauge("test.timer.max", 3000f)));
        verify(_request).addGauge(argThat(hasGauge("test.timer.p95", 3000f)));
        verify(_request).addGauge(argThat(hasGauge("test.meter.count", 100)));
        verify(_request).addGauge(argThat(hasGauge("test.gauge", 50)));

        // Send was called exactly once
        verify(_request).send();

        verifyNoMoreInteractions(_request);
    }

    @Test
    public void testExpansionFilterExclusion() throws Exception {
        String json =
                "{" +
                        "\"type\": \"datadogExpansionFiltered\"," +
                        "\"host\": \"test-host\"," +
                        "\"excludeExpansions\": [\"min\", \"max\", \"p75\", \"p95\", \"p98\", \"p99\", \"p999\"]," +
                        "\"transport\": {" +
                        "\"type\": \"http\"," +
                        "\"apiKey\": \"12345\"" +
                        "}" +
                        "}";


        ScheduledReporter reporter = createReporter(json);

        // Create a representative type.
        Histogram histogram = _metricRegistry.histogram("test.histogram");
        histogram.update(1);
        histogram.update(2);
        histogram.update(3);

        reporter.report();

        // Verify only the desired metrics were sent.  Notably min, max, and the nth percentiles should be absent.
        verify(_request).addGauge(argThat(hasGauge("test.histogram.count", 3)));
        verify(_request).addGauge(argThat(hasGauge("test.histogram.mean", 2)));
        verify(_request).addGauge(argThat(hasGauge("test.histogram.median", 2)));
        verify(_request).addGauge(argThat(hasGauge("test.histogram.stddev", 1.0)));

        // Send was called exactly once
        verify(_request).send();

        verifyNoMoreInteractions(_request);
    }

    private ScheduledReporter createReporter(String json)
            throws Exception {
        ObjectMapper objectMapper = Jackson.newObjectMapper();
        ReporterFactory reporterFactory = objectMapper.readValue(json, ReporterFactory.class);

        assertTrue(reporterFactory instanceof DatadogExpansionFilteredReporterFactory);
        DatadogExpansionFilteredReporterFactory datadogReporterFactory = (DatadogExpansionFilteredReporterFactory) reporterFactory;

        // Replace the transport with our own mock for testing

        Transport transport = mock(Transport.class);
        when(transport.prepare()).thenReturn(_request);

        AbstractTransportFactory transportFactory = mock(AbstractTransportFactory.class);
        when(transportFactory.build()).thenReturn(transport);

        datadogReporterFactory.setTransport(transportFactory);

        // Build the reporter
        return datadogReporterFactory.build(_metricRegistry);
    }

    private static ArgumentMatcher<DatadogGauge> hasGauge(final String metricName, final Number value) {
        return new ArgumentMatcher<DatadogGauge>() {
            @Override
            public boolean matches(DatadogGauge gauge) {
                return metricName.equals(gauge.getMetric()) && value.floatValue() == gauge.getPoints().get(0).get(1).floatValue();
            }

            @Override
            public String toString() {
                return "metric " + metricName + " has value " + value;
            }
        };
    }
}
