package com.bazaarvoice.emodb.web.scanner.writer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.bazaarvoice.emodb.queue.core.ByteBufferInputStream;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
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
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
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
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
            final ByteBuffer contents = ByteBuffer.allocate(128);

            Map<String, String> expectedTags = ImmutableMap.of(
                    "emodb:intrinsic:table", "test:table", "emodb:intrinsic:placement", "p0",
                    "emodb:template:type", "test:type", "emodb:template:client", "test:client");

            AmazonS3 amazonS3 = mock(AmazonS3.class);
            when(amazonS3.putObject(argThat(putsStashFile(
                    "test-bucket", "scan/test~table/test~table-00-0000000000000001-1.json.gz", expectedTags))))
                    .thenAnswer(new Answer<PutObjectResult>() {
                        @Override
                        public PutObjectResult answer(InvocationOnMock invocation) throws Throwable {
                            // Temporary file gets deleted by the writer, so we need to capture the contents at the
                            // time putObject() is called instead of using an argument captor later.
                            PutObjectRequest request = (PutObjectRequest) invocation.getArguments()[0];
                            contents.put(Files.toByteArray(request.getFile()));
                            PutObjectResult result = new PutObjectResult();
                            result.setETag("etag");
                            return result;
                        }
                    });

            S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), metricRegistry, amazonS3, uploadService);
            ShardWriter shardWriter = scanWriter.writeShardRows(
                    new ShardMetadata("test:table", "p0", ImmutableMap.of("type", "test:type", "client", "test:client"), 0, 1));
            shardWriter.getOutputStream().write("This is a test line".getBytes(Charsets.UTF_8));
            shardWriter.closeAndTransferAysnc(Optional.of(1));

            verifyAllTransfersComplete(scanWriter, uploadService);
            verify(amazonS3).putObject(argThat(putsStashFile(
                    "test-bucket", "scan/test~table/test~table-00-0000000000000001-1.json.gz", expectedTags)));
            verifyNoMoreInteractions(amazonS3);

            contents.flip();
            try (Reader in = new InputStreamReader(new GzipCompressorInputStream(new ByteBufferInputStream(contents)))) {
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

        S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), metricRegistry, amazonS3, uploadService);
        scanWriter.setRetryDelay(Duration.millis(10));

        try {
            ShardWriter shardWriter = scanWriter.writeShardRows(
                    new ShardMetadata("testtable", "p0", ImmutableMap.of(), 0, 1));
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

            Map<String, String> expectedTags = ImmutableMap.of(
                    "emodb:intrinsic:table", "table1", "emodb:intrinsic:placement", "p0");

            AmazonS3 amazonS3 = mock(AmazonS3.class);
            when(amazonS3.putObject(argThat(putsStashFile(
                    "test-bucket", "scan/table1/table1-00-0000000000000001-1.json.gz", expectedTags))))
                    .thenReturn(putObjectResult);

            S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), new MetricRegistry(), amazonS3, uploadService);

            ShardWriter shardWriter[] = new ShardWriter[2];

            for (int i=0; i < 2; i++) {
                shardWriter[i] = scanWriter.writeShardRows(
                        new ShardMetadata("table" + i, "p0", ImmutableMap.of(), 0, i));
                shardWriter[i].getOutputStream().write("line0\n".getBytes(Charsets.UTF_8));
            }

            // Simulate canceling shardWriter[0] in response to a failure.
            shardWriter[0].closeAndCancel();
            // Close shardWriter[1] normally
            shardWriter[1].closeAndTransferAysnc(Optional.of(1));

            verifyAllTransfersComplete(scanWriter, uploadService);
            verify(amazonS3).putObject(argThat(putsStashFile(
                    "test-bucket", "scan/table1/table1-00-0000000000000001-1.json.gz", expectedTags)));
            verifyNoMoreInteractions(amazonS3);
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

            Map<String, String> expectedTags = ImmutableMap.of(
                    "emodb:intrinsic:table", "table0", "emodb:intrinsic:placement", "p0");

            AmazonS3 amazonS3 = mock(AmazonS3.class);
            when(amazonS3.putObject(argThat(putsStashFile(
                    "test-bucket", "scan/table0/table0-00-0000000000000000-1.json.gz", expectedTags))))
                    .thenReturn(putObjectResult);

            S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), new MetricRegistry(), amazonS3, uploadService);

            ShardWriter shardWriter[] = new ShardWriter[2];

            for (int i=0; i < 2; i++) {
                shardWriter[i] = scanWriter.writeShardRows(
                        new ShardMetadata("table" + i, "p0", ImmutableMap.of(), 0, i));
                shardWriter[i].getOutputStream().write("line0\n".getBytes(Charsets.UTF_8));
            }

            // Simulate closing shardWriter[0] but not shardWriter[1]
            shardWriter[0].closeAndTransferAysnc(Optional.of(1));

            scanWriter.close();

            verifyAllTransfersComplete(scanWriter, uploadService);
            verify(amazonS3).putObject(argThat(putsStashFile(
                    "test-bucket", "scan/table0/table0-00-0000000000000000-1.json.gz", expectedTags)));
            verifyNoMoreInteractions(amazonS3);
        } finally {
            uploadService.shutdownNow();
        }
    }

    @Test
    public void testWriteWithTooLongTableAttribute()
            throws Exception {
        URI baseUri = URI.create("s3://test-bucket/scan");
        ScheduledExecutorService uploadService = Executors.newScheduledThreadPool(2);

        try {
            PutObjectResult putObjectResult = new PutObjectResult();
            putObjectResult.setETag("dummy-etag");

            // The following value is 257 characters long, one more than the maximum length of 256 characters
            // allowed by S3 for an object tag.  The expected behavior is that the value is replaced with an
            // MD5 checksum of the value in the format given in "expectedHash", which has been pre-computed.
            String tooLongValue = "4Fogc3nGqRU5aLpZ0Vng1cSft1NycctCFVPT5LwJy9TfPMZCqbhE2Z7gCoRhGgBBoODVQlO6gl2TqpBhSm81" +
                    "RWb69KFLDKP4JXmlMaLt1G0llTphC30bj9fGyOxzSt9G7vJZGDam9laaPN1gFeAzniM1Kxb7ge89iR3HH9w5tIIVu9W0mEVKX2" +
                    "nexWlXtN76u7lZxcMsZfzeEMesqTIHZfmjhKBkAxHnIsCAMBwVR0nKGEBG0AIOwBEIaJazUe68X";
            String expectedHash = "emodb:md5:7d75845c0dc918078ba161d3d087a5a7";

            Map<String, String> expectedTags = ImmutableMap.of(
                    "emodb:intrinsic:table", "test:table", "emodb:intrinsic:placement", "p0",
                    "emodb:template:key", expectedHash);

            AmazonS3 amazonS3 = mock(AmazonS3.class);
            when(amazonS3.putObject(argThat(putsStashFile(
                    "test-bucket", "scan/test~table/test~table-00-0000000000000001-1.json.gz", expectedTags))))
                    .thenReturn(putObjectResult);

            S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), new MetricRegistry(), amazonS3, uploadService);
            ShardWriter shardWriter = scanWriter.writeShardRows(
                    new ShardMetadata("test:table", "p0", ImmutableMap.of("key", tooLongValue), 0, 1));
            shardWriter.getOutputStream().write("line0\n".getBytes(Charsets.UTF_8));
            shardWriter.closeAndTransferAysnc(Optional.of(1));

            scanWriter.close();

            verifyAllTransfersComplete(scanWriter, uploadService);
            verify(amazonS3).putObject(argThat(putsStashFile(
                    "test-bucket", "scan/test~table/test~table-00-0000000000000001-1.json.gz", expectedTags)));
            verifyNoMoreInteractions(amazonS3);
        } finally {
            uploadService.shutdownNow();
        }
    }

    @Test
    public void testWriteWithTooManyTableAttributes()
            throws Exception {
        URI baseUri = URI.create("s3://test-bucket/scan");
        ScheduledExecutorService uploadService = Executors.newScheduledThreadPool(2);

        try {
            PutObjectResult putObjectResult = new PutObjectResult();
            putObjectResult.setETag("dummy-etag");

            // An S3 object is allowed to have at most 10 tags.  Stash uses two of these for table and placement
            // intrinsics, leaving only 8 slots for table attributes.  For this test create 9 table attributes.
            // Stash should sort by attribute name and only include the first 8.   Use a linked hash map with keys
            // inserted in reverse order to verify sorting.
            Map<String, Object> allTableAttributes = Maps.newLinkedHashMap();
            for (int i=8; i >= 0; i--) {
                allTableAttributes.put("key" + i, "value" + i);
            }

            Map<String, String> expectedTags = Maps.newHashMap();
            expectedTags.put("emodb:intrinsic:table", "test:table");
            expectedTags.put("emodb:intrinsic:placement", "p0");
            for (int i=0; i < 8; i++) {
                expectedTags.put("emodb:template:key" + i, "value" + i);
            }

            AmazonS3 amazonS3 = mock(AmazonS3.class);
            when(amazonS3.putObject(argThat(putsStashFile(
                    "test-bucket", "scan/test~table/test~table-00-0000000000000001-1.json.gz", expectedTags))))
                    .thenReturn(putObjectResult);

            S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), new MetricRegistry(), amazonS3, uploadService);
            ShardWriter shardWriter = scanWriter.writeShardRows(
                    new ShardMetadata("test:table", "p0", allTableAttributes, 0, 1));
            shardWriter.getOutputStream().write("line0\n".getBytes(Charsets.UTF_8));
            shardWriter.closeAndTransferAysnc(Optional.of(1));

            scanWriter.close();

            verifyAllTransfersComplete(scanWriter, uploadService);
            verify(amazonS3).putObject(argThat(putsStashFile(
                    "test-bucket", "scan/test~table/test~table-00-0000000000000001-1.json.gz", expectedTags)));
            verifyNoMoreInteractions(amazonS3);
        } finally {
            uploadService.shutdownNow();
        }
    }

    @Test
    public void testWriteWithEncodedTableAttributes()
            throws Exception {
        URI baseUri = URI.create("s3://test-bucket/scan");
        ScheduledExecutorService uploadService = Executors.newScheduledThreadPool(2);

        try {
            PutObjectResult putObjectResult = new PutObjectResult();
            putObjectResult.setETag("dummy-etag");

            // The template key and value contain numerous special characters that are encoded in the expected tags.
            Map<String, String> expectedTags = ImmutableMap.of(
                    "emodb:intrinsic:table", "test:table", "emodb:intrinsic:placement", "p0",
                    "emodb:template:key/0040/0021", "x/002fy/003fz");

            AmazonS3 amazonS3 = mock(AmazonS3.class);
            when(amazonS3.putObject(argThat(putsStashFile(
                    "test-bucket", "scan/test~table/test~table-00-0000000000000001-1.json.gz", expectedTags))))
                    .thenReturn(putObjectResult);

            S3ScanWriter scanWriter = new S3ScanWriter(1, baseUri, Optional.of(2), new MetricRegistry(), amazonS3, uploadService);
            ShardWriter shardWriter = scanWriter.writeShardRows(
                    new ShardMetadata("test:table", "p0", ImmutableMap.of("key@!", "x/y?z"), 0, 1));
            shardWriter.getOutputStream().write("line0\n".getBytes(Charsets.UTF_8));
            shardWriter.closeAndTransferAysnc(Optional.of(1));

            scanWriter.close();

            verifyAllTransfersComplete(scanWriter, uploadService);
            verify(amazonS3).putObject(argThat(putsStashFile(
                    "test-bucket", "scan/test~table/test~table-00-0000000000000001-1.json.gz", expectedTags)));
            verifyNoMoreInteractions(amazonS3);
        } finally {
            uploadService.shutdownNow();
        }
    }

    private Matcher<PutObjectRequest> putsStashFile(final String bucket, final String key, final Map<String, String> tags) {
        return new BaseMatcher<PutObjectRequest>() {
            @Override
            public boolean matches(Object item) {
                PutObjectRequest request = (PutObjectRequest) item;

                return request != null
                        && request.getBucketName().equals(bucket)
                        && request.getKey().equals(key)
                        && toMap(request.getCustomRequestHeaders().get("x-amz-tagging")).equals(tags);
            }

            private Map<String, String> toMap(String tagHeader) {
                // Splitting by '&' and '=' isn't perfect, but it works with all of our test inputs.
                try {
                    Map<String, String> map = Maps.newLinkedHashMap();
                    for (String keyValue : tagHeader.split("&")) {
                        int eq = keyValue.indexOf('=');
                        map.put(URLDecoder.decode(keyValue.substring(0, eq), "UTF-8"),
                                URLDecoder.decode(keyValue.substring(eq + 1), "UTF-8"));
                    }
                    return map;
                } catch (UnsupportedEncodingException e) {
                    // Shouldn't happen with UTF-8
                    throw Throwables.propagate(e);
                }
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
