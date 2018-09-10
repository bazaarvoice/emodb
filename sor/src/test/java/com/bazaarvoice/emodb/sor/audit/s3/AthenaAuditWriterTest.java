package com.bazaarvoice.emodb.sor.audit.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.bazaarvoice.emodb.common.dropwizard.log.DefaultRateLimitedLogFactory;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLog;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLogFactory;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.core.GracefulShutdownManager;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.util.Size;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.matchers.LessThan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.longThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AthenaAuditWriterTest {

    private final static String BUCKET = "test-bucket";

    private AmazonS3 _s3;
    private Multimap<String, Map<String, Object>> _uploadedAudits = ArrayListMultimap.create();
    private AthenaAuditWriter _writer;
    private File _tempStagingDir;
    private ScheduledExecutorService _auditService;
    private ExecutorService _fileTransferService;
    private Runnable _processQueuedAudits;
    private Runnable _doLogFileMaintenance;
    private Instant _now;
    private Clock _clock;

    @BeforeMethod
    public void setUp() {
        _s3 = mock(AmazonS3.class);
        when(_s3.putObject(eq(BUCKET), anyString(), any(File.class))).then(invocationOnMock -> {
            // The file will be deleted after the put object returns successfully, so capture the contents now
            File file = (File) invocationOnMock.getArguments()[2];
            try (FileInputStream fileIn = new FileInputStream(file);
                 GzipCompressorInputStream in = new GzipCompressorInputStream(fileIn);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(in, Charsets.UTF_8))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    Map<String, Object> auditJson = JsonHelper.fromJson(line, new TypeReference<Map<String, Object>>() {});
                    _uploadedAudits.put((String) invocationOnMock.getArguments()[1], auditJson);
                }
            }

            PutObjectResult result = new PutObjectResult();
            result.setETag(file.getName());
            return result;
        });

        _tempStagingDir = Files.createTempDir();

        // Start with some default time; individual tests can override as necessary
        _now = Instant.from(ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC));

        _clock = mock(Clock.class);
        when(_clock.millis()).then(ignore -> _now.toEpochMilli());
        when(_clock.instant()).then(ignore -> _now);
    }

    @AfterMethod
    public void cleanUp() {
        _uploadedAudits.clear();

        if (_tempStagingDir.exists()) {
            File[] files = _tempStagingDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
            }
            _tempStagingDir.delete();
        }
    }

    private AthenaAuditWriter createWriter(String s3Path, String prefix, long maxFileSize, Duration maxBatchTime) {

        _auditService = mock(ScheduledExecutorService.class);
        _fileTransferService = mock(ExecutorService.class);

        AthenaAuditWriter writer = new AthenaAuditWriter(_s3, BUCKET, s3Path, maxFileSize,
                maxBatchTime, _tempStagingDir, prefix, Jackson.newObjectMapper(), _clock, true,
                log -> mock(RateLimitedLog.class), mock(MetricRegistry.class), _auditService, _fileTransferService);

        // On start two services should have been submitted: one to poll the audit queue and one to close log files and
        // initiate transfers.  Capture them now.

        ArgumentCaptor<Runnable> processQueuedAudits = ArgumentCaptor.forClass(Runnable.class);
        verify(_auditService).scheduleWithFixedDelay(processQueuedAudits.capture(), eq(0L), eq(1L), eq(TimeUnit.SECONDS));
        _processQueuedAudits = processQueuedAudits.getValue();

        ArgumentCaptor<Runnable> doLogFileMaintenance = ArgumentCaptor.forClass(Runnable.class);
        verify(_auditService).scheduleAtFixedRate(
                doLogFileMaintenance.capture(),
                longThat(new LessThan<>(maxBatchTime.toMillis()+1)),
                eq(maxBatchTime.toMillis()), eq(TimeUnit.MILLISECONDS));
        _doLogFileMaintenance = doLogFileMaintenance.getValue();

        // Normally should avoid resetting mocks but it's already embedded in the writer.
        reset(_auditService);

        return writer;
    }

    @Test
    public void testSimpleAudits() throws Exception {
        String prefix = "emodb-audit";
        long maxFileSize = Size.megabytes(1).toBytes();
        Duration maxBatchTime = Duration.ofSeconds(10);

        AthenaAuditWriter writer = createWriter("path/to/test", prefix, maxFileSize, maxBatchTime);

        long auditTime = _now.toEpochMilli();
        for (int i=0; i < 10; i++) {
            Audit audit = new AuditBuilder().setComment("comment" + i).set("custom", "custom" + i).build();
            writer.persist("test:table", "key" + i, audit, auditTime);
        }

        // Simulate the background thread processing these audits
        _processQueuedAudits.run();

        // Move time ahead 10 seconds to force the log to write
        _now = _now.plusSeconds(10);
        _doLogFileMaintenance.run();

        ArgumentCaptor<Runnable> fileTransferRunnable = ArgumentCaptor.forClass(Runnable.class);
        verify(_fileTransferService).submit(fileTransferRunnable.capture());
        fileTransferRunnable.getValue().run();

        assertEquals(_uploadedAudits.keySet().size(), 1);
        String key = _uploadedAudits.keySet().iterator().next();
        assertTrue(key.matches("path/to/test/date=20180101/emodb-audit.20180101000000.[a-f0-9\\-]{36}.log.gz"));

        Collection<Map<String, Object>> auditMaps = _uploadedAudits.get(key);
        assertEquals(auditMaps.size(), 10);

        int i = 0;
        for (Map<String, Object> auditMap : auditMaps) {
            assertEquals(auditMap.get("tablename"), "test:table");
            assertEquals(auditMap.get("key"), "key" + i);
            assertEquals(auditMap.get("time"), auditTime);
            assertEquals(auditMap.get("comment"), "comment" + i);
            assertEquals(auditMap.get("custom"), "{\"custom\":\"custom" + i + "\"}");
            i += 1;
        }
    }
}