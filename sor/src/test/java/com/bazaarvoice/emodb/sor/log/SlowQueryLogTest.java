package com.bazaarvoice.emodb.sor.log;

import com.bazaarvoice.emodb.sor.core.Expanded;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.io.Files;
import io.dropwizard.jackson.Jackson;
import org.testng.annotations.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Simple test to validate that the slow query log is, in fact, logging.
 */
public class SlowQueryLogTest {

    @Test
    public void testSlowQueryLog() throws Exception {
        File logFile = File.createTempFile("slow-query", ".log");
        try {
            ObjectMapper objectMapper = Jackson.newObjectMapper();
            SlowQueryLogConfiguration config = objectMapper.readValue(
                    "{" +
                        "\"tooManyDeltasThreshold\": 20," +
                        "\"file\": {" +
                            "\"type\": \"file\"," +
                            "\"currentLogFilename\": \"" + logFile.getAbsolutePath() + "\"," +
                            "\"archive\": false" +
                        "}" +
                    "}",
                    SlowQueryLogConfiguration.class);

            SlowQueryLog log = new LogbackSlowQueryLogProvider(config, new MetricRegistry()).get();

            Expanded expanded = mock(Expanded.class);
            when(expanded.getNumPersistentDeltas()).thenReturn(100);
            when(expanded.getNumDeletedDeltas()).thenReturn(2L);

            log.log("test:table", "test-key", expanded);

            // Logging is asynchronous; allow up to 10 seconds for the message to be logged
            Stopwatch stopwatch = Stopwatch.createStarted();
            while (logFile.length() == 0 && stopwatch.elapsed(TimeUnit.SECONDS) < 10) {
                Thread.sleep(100);
            }

            String line = Files.readFirstLine(logFile, Charsets.UTF_8);
            assertNotNull(line);
            assertTrue(line.endsWith("Too many deltas: 100 2 test:table test-key"));
        } finally {
            //noinspection ResultOfMethodCallIgnored
            logFile.delete();

            // Reset everything so future tests that use slow query logging don't log to the file
            new LogbackSlowQueryLogProvider(new SlowQueryLogConfiguration(), new MetricRegistry()).get();
        }
    }
}
