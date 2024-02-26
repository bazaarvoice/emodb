package com.bazaarvoice.emodb.web.config;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.metrics.DatadogReporterFactory;
import io.dropwizard.metrics.ReporterFactory;
import org.coursera.metrics.datadog.DatadogReporter;
import org.coursera.metrics.datadog.model.DatadogGauge;
import org.coursera.metrics.datadog.transport.AbstractTransportFactory;
import org.coursera.metrics.datadog.transport.Transport;
import org.mockito.ArgumentMatcher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
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
                    "\"type\": \"datadog\"," +
                    "\"host\": \"test-host\"," +
                    "\"expansions\": [\"COUNT\", \"MIN\", \"MAX\", \"P95\"]," +
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
    @Ignore
    public void testExpansionFilterExclusion() throws Exception {
        String json =
                "{" +
                    "\"type\": \"datadog\"," +
                    "\"host\": \"test-host\"," +
                    "\"expansions\": [\"COUNT\", \"MEAN\", \"MEDIAN\", \"STD_DEV\"]," +
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
        verify(_request).addGauge(argThat(hasGauge("test.histogram.stddev", 1)));

        // Send was called exactly once
        verify(_request).send();

        verifyNoMoreInteractions(_request);
    }

    private ScheduledReporter createReporter(String json)
            throws Exception {
        ObjectMapper objectMapper = Jackson.newObjectMapper();
        ReporterFactory reporterFactory = objectMapper.readValue(json, ReporterFactory.class);

        assertTrue(reporterFactory instanceof DatadogReporterFactory);

        // Replace the transport with our own mock for testing

        Transport transport = mock(Transport.class);
        when(transport.prepare()).thenReturn(_request);

        AbstractTransportFactory transportFactory = mock(AbstractTransportFactory.class);
        when(transportFactory.build()).thenReturn(transport);

        setField(reporterFactory, "transport", transportFactory);

        // Build the reporter
        DatadogReporter.forRegistry(_metricRegistry)
                .withTransport(transport)
                .build();
        return reporterFactory.build(_metricRegistry);
    }

    /**
     * Sets a field value on a given object
     *
     * @param targetObject the object to set the field value on
     * @param fieldName    exact name of the field
     * @param fieldValue   value to set on the field
     * @return true if the value was successfully set, false otherwise
     */
    public static boolean setField(Object targetObject, String fieldName, Object fieldValue) {
        Field field;
        try {
            field = targetObject.getClass().getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            field = null;
        }
        Class superClass = targetObject.getClass().getSuperclass();
        while (field == null && superClass != null) {
            try {
                field = superClass.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                superClass = superClass.getSuperclass();
            }
        }
        if (field == null) {
            return false;
        }
        field.setAccessible(true);
        try {
            field.set(targetObject, fieldValue);
            return true;
        } catch (IllegalAccessException e) {
            return false;
        }
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
