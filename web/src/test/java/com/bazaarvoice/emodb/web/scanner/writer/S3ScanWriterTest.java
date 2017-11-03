package com.bazaarvoice.emodb.web.scanner.writer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.bazaarvoice.emodb.queue.core.ByteBufferInputStream;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.joda.time.Duration;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static junit.framework.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
                    .thenAnswer(new Answer<PutObjectResult>() {
                        @Override
                        public PutObjectResult answer(InvocationOnMock invocation) throws Throwable {
                            PutObjectRequest request = (PutObjectRequest) invocation.getArguments()[0];
                            assertEquals("test-bucket", request.getBucketName());
                            putObjects.put(request.getKey(), ByteBuffer.wrap(Files.toByteArray(request.getFile())));
                            PutObjectResult result = new PutObjectResult();
                            result.setETag("etag");
                            return result;
                        }
                    });

            AmazonS3Provider amazonS3Provider = mock(AmazonS3Provider.class);
            when(amazonS3Provider.getS3ClientForBucket("test-bucket")).thenReturn(amazonS3);

            S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), metricRegistry, amazonS3Provider, uploadService);
            ShardWriter shardWriter = scanWriter.writeShardRows("testtable", "p0", 0, 1);
            shardWriter.getOutputStream().write("This is a test line".getBytes(Charsets.UTF_8));
            shardWriter.closeAndTransferAysnc(Optional.of(1));

            verifyAllTransfersComplete(scanWriter, uploadService);

            ByteBuffer byteBuffer = putObjects.get("scan/testtable/testtable-00-0000000000000001-1.json.gz");
            byteBuffer.position(0);
            try (Reader in = new InputStreamReader(new GzipCompressorInputStream(new ByteBufferInputStream(byteBuffer)))) {
                assertEquals("This is a test line", CharStreams.toString(in));
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

        S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), metricRegistry, amazonS3Provider, uploadService);
        scanWriter.setRetryDelay(Duration.millis(10));

        try {
            ShardWriter shardWriter = scanWriter.writeShardRows("testtable", "p0", 0, 1);
            shardWriter.getOutputStream().write("This is a test line".getBytes(Charsets.UTF_8));
            shardWriter.closeAndTransferAysnc(Optional.of(1));

            scanWriter.waitForAllTransfersComplete(Duration.standardSeconds(10));
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

            S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), new MetricRegistry(), amazonS3Provider, uploadService);

            ShardWriter shardWriter[] = new ShardWriter[2];

            for (int i=0; i < 2; i++) {
                shardWriter[i] = scanWriter.writeShardRows("table" + i, "p0", 0, i);
                shardWriter[i].getOutputStream().write("line0\n".getBytes(Charsets.UTF_8));
            }

            // Simulate canceling shardWriter[0] in response to a failure.
            shardWriter[0].closeAndCancel();
            // Close shardWriter[1] normally
            shardWriter[1].closeAndTransferAysnc(Optional.of(1));

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

            S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), new MetricRegistry(), amazonS3Provider, uploadService);

            ShardWriter shardWriter[] = new ShardWriter[2];

            for (int i=0; i < 2; i++) {
                shardWriter[i] = scanWriter.writeShardRows("table" + i, "p0", 0, i);
                shardWriter[i].getOutputStream().write("line0\n".getBytes(Charsets.UTF_8));
            }

            // Simulate closing shardWriter[0] but not shardWriter[1]
            shardWriter[0].closeAndTransferAysnc(Optional.of(1));

            scanWriter.close();

            verifyAllTransfersComplete(scanWriter, uploadService);
        } finally {
            uploadService.shutdownNow();
        }
    }

    private Matcher<PutObjectRequest> putsIntoBucket(final String bucket) {
        return new BaseMatcher<PutObjectRequest>() {
            @Override
            public boolean matches(Object item) {
                PutObjectRequest request = (PutObjectRequest) item;
                return request != null && request.getBucketName().equals(bucket);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("PutObjectRequest for bucket ").appendText(bucket);
            }
        };
    }

    private void verifyAllTransfersComplete(ScanWriter scanWriter, ScheduledExecutorService uploadService)
            throws Exception {
        assertTrue(scanWriter.waitForAllTransfersComplete(Duration.standardSeconds(10)).isComplete(), "All transfers did not complete");

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
