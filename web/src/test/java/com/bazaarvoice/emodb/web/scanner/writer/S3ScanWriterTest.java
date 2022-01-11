package com.bazaarvoice.emodb.web.scanner.writer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.bazaarvoice.emodb.queue.core.ByteBufferInputStream;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.mockito.ArgumentMatcher;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class S3ScanWriterTest {

    @Test
    public void testWrite()
            throws Exception {
        URI baseUri = URI.create("s3://test-bucket/scan");
        ScheduledExecutorService uploadService = Executors.newScheduledThreadPool(2);
        final MetricRegistry metricRegistry = new MetricRegistry();

        try {
            AmazonS3 amazonS3 = mock(AmazonS3.class);

            final Map<String, ByteBuffer> putObjects = Maps.newHashMap();

            when(amazonS3.putObject(any(PutObjectRequest.class)))
                    .thenAnswer((Answer<PutObjectResult>) invocation -> {
                        PutObjectRequest request = (PutObjectRequest) invocation.getArguments()[0];
                        assertEquals(request.getBucketName(), "test-bucket");
                        putObjects.put(request.getKey(), ByteBuffer.wrap(Files.toByteArray(request.getFile())));
                        PutObjectResult result = new PutObjectResult();
                        result.setETag("etag");
                        return result;
                    });

            Map<String, Object> doc = ImmutableMap.of("type", "review", "rating", 5);

            AmazonS3Provider amazonS3Provider = mock(AmazonS3Provider.class);
            when(amazonS3Provider.getS3ClientForBucket("test-bucket")).thenReturn(amazonS3);

            S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), metricRegistry, amazonS3Provider, uploadService, new ObjectMapper());
            ScanDestinationWriter scanDestinationWriter = scanWriter.writeShardRows("testtable", "p0", 0, 1);
            scanDestinationWriter.writeDocument(doc);
            scanDestinationWriter.closeAndTransferAsync(Optional.of(1));

            verifyAllTransfersComplete(scanWriter, uploadService);

            ByteBuffer byteBuffer = putObjects.get("scan/testtable/testtable-00-0000000000000001-1.json.gz");
            byteBuffer.position(0);
            try (Reader in = new InputStreamReader(new GzipCompressorInputStream(new ByteBufferInputStream(byteBuffer)))) {
                assertEquals(CharStreams.toString(in).trim(), "{\"type\":\"review\",\"rating\":5}");
            }
        } finally {
            uploadService.shutdownNow();
        }
    }

    @Test
    public void testWriteWithError()
            throws Exception {
        URI baseUri = URI.create("s3://test-bucket/scan");
        ScheduledExecutorService uploadService = Executors.newScheduledThreadPool(2);
        AmazonS3 amazonS3 = mock(AmazonS3.class);
        final MetricRegistry metricRegistry = new MetricRegistry();

        when(amazonS3.putObject(any(PutObjectRequest.class)))
                .thenThrow(new AmazonClientException("Simulated transfer failure"));

        AmazonS3Provider amazonS3Provider = mock(AmazonS3Provider.class);
        when(amazonS3Provider.getS3ClientForBucket("test-bucket")).thenReturn(amazonS3);

        S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), metricRegistry, amazonS3Provider, uploadService, new ObjectMapper());
        scanWriter.setRetryDelay(Duration.ofMillis(10));

        try {
            ScanDestinationWriter scanDestinationWriter = scanWriter.writeShardRows("testtable", "p0", 0, 1);
            scanDestinationWriter.writeDocument(ImmutableMap.of("type", "review", "rating", 5));
            scanDestinationWriter.closeAndTransferAsync(Optional.of(1));

            scanWriter.waitForAllTransfersComplete(Duration.ofSeconds(10));
            fail("No transfer exception thrown");
        } catch (IOException e) {
            assertTrue(e.getCause() instanceof AmazonClientException);
            assertEquals(e.getCause().getMessage(), "Simulated transfer failure");
        } finally {
            uploadService.shutdownNow();
        }

        // Transfer should have been retried three times
        verify(amazonS3, times(3)).putObject(any(PutObjectRequest.class));
    }

    @Test
    public void testWriteWithCancel()
            throws Exception {
        URI baseUri = URI.create("s3://test-bucket/scan");
        ScheduledExecutorService uploadService = Executors.newScheduledThreadPool(2);

        try {
            PutObjectResult putObjectResult = new PutObjectResult();
            putObjectResult.setETag("dummy-etag");

            AmazonS3 amazonS3 = mock(AmazonS3.class);
            when(amazonS3.putObject(argThat(putsIntoBucket("test-bucket"))))
                    .thenReturn(putObjectResult);

            AmazonS3Provider amazonS3Provider = mock(AmazonS3Provider.class);
            when(amazonS3Provider.getS3ClientForBucket("test-bucket")).thenReturn(amazonS3);

            S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), new MetricRegistry(), amazonS3Provider, uploadService, new ObjectMapper());

            ScanDestinationWriter[] scanDestinationWriters = new ScanDestinationWriter[2];

            for (int i = 0; i < 2; i++) {
                scanDestinationWriters[i] = scanWriter.writeShardRows("table" + i, "p0", 0, i);
                scanDestinationWriters[i].writeDocument(ImmutableMap.of("type", "review", "rating", i));
            }

            // Simulate canceling shardWriter[0] in response to a failure.
            scanDestinationWriters[0].closeAndCancel();
            // Close shardWriter[1] normally
            scanDestinationWriters[1].closeAndTransferAsync(Optional.of(1));

            verifyAllTransfersComplete(scanWriter, uploadService);
        } finally {
            uploadService.shutdownNow();
        }
    }

    @Test
    public void testWriteWithClose()
            throws Exception {

        URI baseUri = URI.create("s3://test-bucket/scan");
        ScheduledExecutorService uploadService = Executors.newScheduledThreadPool(2);

        try {
            PutObjectResult putObjectResult = new PutObjectResult();
            putObjectResult.setETag("dummy-etag");

            AmazonS3 amazonS3 = mock(AmazonS3.class);
            when(amazonS3.putObject(argThat(putsIntoBucket("test-bucket"))))
                    .thenReturn(putObjectResult);

            AmazonS3Provider amazonS3Provider = mock(AmazonS3Provider.class);
            when(amazonS3Provider.getS3ClientForBucket("test-bucket")).thenReturn(amazonS3);

            S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), new MetricRegistry(), amazonS3Provider, uploadService, new ObjectMapper());

            ScanDestinationWriter[] scanDestinationWriters = new ScanDestinationWriter[2];

            for (int i = 0; i < 2; i++) {
                scanDestinationWriters[i] = scanWriter.writeShardRows("table" + i, "p0", 0, i);
                scanDestinationWriters[i].writeDocument(ImmutableMap.of("type", "review", "rating", i));
            }

            // Simulate closing shardWriter[0] but not shardWriter[1]
            scanDestinationWriters[0].closeAndTransferAsync(Optional.of(1));

            scanWriter.close();

            verifyAllTransfersComplete(scanWriter, uploadService);
        } finally {
            uploadService.shutdownNow();
        }
    }

    private ArgumentMatcher<PutObjectRequest> putsIntoBucket(final String bucket) {
        return new ArgumentMatcher<PutObjectRequest>() {
            @Override
            public boolean matches(PutObjectRequest request) {
                return request != null && request.getBucketName().equals(bucket);
            }

            @Override
            public String toString() {
                return "PutObjectRequest for bucket " + bucket;
            }
        };
    }

    private void verifyAllTransfersComplete(ScanWriter scanWriter, ScheduledExecutorService uploadService)
            throws Exception {
        assertTrue(scanWriter.waitForAllTransfersComplete(Duration.ofSeconds(10)).isComplete(), "All transfers did not complete");

        // Give 10 seconds for all threads to be complete
        int activeCount = Integer.MAX_VALUE;
        long timeoutTs = System.currentTimeMillis() + 10000;
        while (activeCount != 0 && System.currentTimeMillis() < timeoutTs) {
            activeCount = ((ScheduledThreadPoolExecutor) uploadService).getActiveCount();
            Thread.sleep(100);
        }
        assertEquals(activeCount, 0);
    }
}
