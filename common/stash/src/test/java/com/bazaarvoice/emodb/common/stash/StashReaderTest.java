package com.bazaarvoice.emodb.common.stash;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.bazaarvoice.emodb.common.json.ISO8601DateFormat;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.SkipException;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;

import static java.lang.String.format;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class StashReaderTest {

    @Test
    public void testGetLatestCreationTime() throws Exception {
        AmazonS3 s3 = mock(AmazonS3.class);

        final AtomicReference<String> latest = new AtomicReference<>("2015-01-01-00-00-00");
        when(s3.getObject(argThat(getsObject("stash-bucket", "stash/test/_LATEST")))).thenAnswer(new Answer<S3Object>() {
            @Override
            public S3Object answer(InvocationOnMock invocation)
                    throws Throwable {
                S3Object s3Object = new S3Object();
                s3Object.setObjectContent(new ByteArrayInputStream(latest.get().getBytes(Charsets.UTF_8)));
                return s3Object;
            }
        });

        // Get the latest
        StandardStashReader reader = new StandardStashReader(URI.create("s3://stash-bucket/stash/test"), s3, Duration.ZERO);
        assertEquals("2015-01-01-00-00-00", reader.getLatest());
        assertEquals(reader.getLatestCreationTime(), new ISO8601DateFormat().parse("2015-01-01T00:00:00Z"));

        // Lock the latest
        reader.lockToLatest();
        // Update the latest
        latest.set("2015-01-02-00-00-00");
        // Make sure the latest is still locked
        assertEquals("2015-01-01-00-00-00", reader.getLatest());
        assertEquals(reader.getLatestCreationTime(), new ISO8601DateFormat().parse("2015-01-01T00:00:00Z"));

        // Unlock the latest
        reader.unlock();
        assertEquals("2015-01-02-00-00-00", reader.getLatest());
        assertEquals(reader.getLatestCreationTime(), new ISO8601DateFormat().parse("2015-01-02T00:00:00Z"));
    }

    @Test
    public void testGetStashStartTime() throws Exception {
        AmazonS3 s3 = mock(AmazonS3.class);

        final String latest = "2015-01-01-00-00-00";
        when(s3.getObject(argThat(getsObject("stash-bucket", "stash/test/_LATEST")))).thenAnswer(new Answer<S3Object>() {
            @Override
            public S3Object answer(InvocationOnMock invocation)
                    throws Throwable {
                S3Object s3Object = new S3Object();
                s3Object.setObjectContent(new ByteArrayInputStream(latest.getBytes(Charsets.UTF_8)));
                return s3Object;
            }
        });

        Date startTime = Date.from(Instant.now().minus(Duration.ofDays(1)));
        String startTimeAsWrittenInSuccessFile = new ISO8601DateFormat().format(startTime);
        final String contents = format("%s\n%s\n%s", startTimeAsWrittenInSuccessFile, "2015-01-01T03:00:00Z", "scanId");
        when(s3.getObject(argThat(getsObject("stash-bucket", "stash/test/" + latest + "/_SUCCESS")))).thenAnswer(new Answer<S3Object>() {
            @Override
            public S3Object answer(InvocationOnMock invocation)
                    throws Throwable {
                S3Object s3Object = new S3Object();
                s3Object.setObjectContent(new ByteArrayInputStream(contents.getBytes(Charsets.UTF_8)));
                return s3Object;
            }
        });

        // Get the stash start timestamp
        StandardStashReader reader = new StandardStashReader(URI.create("s3://stash-bucket/stash/test"), s3, Duration.ZERO);
        assertEquals(reader.getStashCreationTime(), startTime, "Actual stash start timestamp");
    }

    @Test
    public void testListTables() {
        AmazonS3 s3 = mock(AmazonS3.class);
        when(s3.getObject(argThat(getsObject("stash-bucket", "stash/test/_LATEST")))).thenAnswer(new Answer<S3Object>() {
            @Override
            public S3Object answer(InvocationOnMock invocation)
                    throws Throwable {
                S3Object s3Object = new S3Object();
                s3Object.setObjectContent(new ByteArrayInputStream("2015-01-01-00-00-00".getBytes(Charsets.UTF_8)));
                return s3Object;
            }
        });

        ObjectListing listing = new ObjectListing();
        listing.setCommonPrefixes(ImmutableList.of(
                "stash/test/2015-01-01-00-00-00/table-one/", "stash/test/2015-01-01-00-00-00/table_two/",
                "stash/test/2015-01-01-00-00-00/table~three/", "stash/test/2015-01-01-00-00-00/table.four/"));
        listing.setTruncated(false);
        when(s3.listObjects(argThat(listObjectRequest("stash-bucket", "stash/test/2015-01-01-00-00-00/", null))))
                .thenReturn(listing);

        StandardStashReader reader = new StandardStashReader(URI.create("s3://stash-bucket/stash/test"), s3, Duration.ZERO);
        List<StashTable> tables = ImmutableList.copyOf(reader.listTables());

        assertEquals(tables, ImmutableList.of(
                new StashTable("stash-bucket", "stash/test/2015-01-01-00-00-00/table-one/", "table-one"),
                new StashTable("stash-bucket", "stash/test/2015-01-01-00-00-00/table_two/", "table_two"),
                new StashTable("stash-bucket", "stash/test/2015-01-01-00-00-00/table~three/", "table:three"),
                new StashTable("stash-bucket", "stash/test/2015-01-01-00-00-00/table.four/", "table.four")));
    }

    @Test
    public void testListTableMetadata() {
        AmazonS3 s3 = mock(AmazonS3.class);
        when(s3.getObject(argThat(getsObject("stash-bucket", "stash/test/_LATEST")))).thenAnswer(new Answer<S3Object>() {
            @Override
            public S3Object answer(InvocationOnMock invocation)
                    throws Throwable {
                S3Object s3Object = new S3Object();
                s3Object.setObjectContent(new ByteArrayInputStream("2015-01-01-00-00-00".getBytes(Charsets.UTF_8)));
                return s3Object;
            }
        });

        String nextMarker = "1";
        ObjectListing listing = new ObjectListing();
        listing.setCommonPrefixes(ImmutableList.<String>of());
        listing.setNextMarker(nextMarker);

        // Add an initial summary for the _SUCCESS file
        S3ObjectSummary objectSummary = new S3ObjectSummary();
        objectSummary.setBucketName("stash-bucket");
        objectSummary.setKey(String.format("stash/test/2015-01-01-00-00-00/_SUCCESS"));
        objectSummary.setSize(10);
        listing.getObjectSummaries().add(objectSummary);

        // Simulate 100 tables, each with 8 gzip files of content
        for (int t=0; t < 100; t++) {
            for (int f=0; f < 8; f++) {
                objectSummary = new S3ObjectSummary();
                objectSummary.setBucketName("stash-bucket");
                objectSummary.setKey(
                        String.format("stash/test/2015-01-01-00-00-00/table~client%d/table~client%d-%d.gz", t, t, f));
                objectSummary.setSize(10 * (f + 1));
                listing.getObjectSummaries().add(objectSummary);

                // Every 7 files force a continuation in the next request.  This splits the listing such that
                // some tables complete in a single request and some are split.
                if (listing.getObjectSummaries().size() == 7) {
                    listing.setTruncated(true);
                    listing.setNextMarker(nextMarker);
                    when(s3.listObjects(argThat(listObjectRequest("stash-bucket", "stash/test/2015-01-01-00-00-00/", listing.getMarker()))))
                            .thenReturn(listing);

                    // Start the next object listing
                    listing = new ObjectListing();
                    listing.setCommonPrefixes(ImmutableList.<String>of());
                    listing.setMarker(nextMarker);
                    nextMarker = String.valueOf(Integer.parseInt(nextMarker) + 1);
                }
            }
        }

        listing.setTruncated(false);
        listing.setNextMarker(null);
        when(s3.listObjects(argThat(listObjectRequest("stash-bucket", "stash/test/2015-01-01-00-00-00/", listing.getMarker()))))
                .thenReturn(listing);

        StandardStashReader reader = new StandardStashReader(URI.create("s3://stash-bucket/stash/test"), s3, Duration.ZERO);
        List<StashTableMetadata> tables = ImmutableList.copyOf(reader.listTableMetadata());
        assertEquals(tables.size(), 100);

        for (int t=0; t < 100; t++) {
            StashTableMetadata tableMetadata = tables.get(t);
            String expectedPrefix = String.format("stash/test/2015-01-01-00-00-00/table~client%d/", t);
            String expectedTableName = String.format("table:client%d", t);

            assertEquals(tableMetadata.getBucket(), "stash-bucket");
            assertEquals(tableMetadata.getPrefix(), expectedPrefix);
            assertEquals(tableMetadata.getTableName(), expectedTableName);
            assertEquals(tableMetadata.getFiles().size(), 8);

            for (int f=0; f < 8; f++) {
                StashFileMetadata fileMetadata = tableMetadata.getFiles().get(f);
                String expectedKey = String.format("stash/test/2015-01-01-00-00-00/table~client%d/table~client%d-%d.gz", t, t, f);
                long expectedSize = 10 * (f + 1);

                assertEquals(fileMetadata.getBucket(), "stash-bucket");
                assertEquals(fileMetadata.getKey(), expectedKey);
                assertEquals(fileMetadata.getSize(), expectedSize);
            }
        }
    }

    @Test
    public void testGetTableMetadata() throws Exception {
        AmazonS3 s3 = mock(AmazonS3.class);
        when(s3.getObject(argThat(getsObject("stash-bucket", "stash/test/_LATEST")))).thenAnswer(new Answer<S3Object>() {
            @Override
            public S3Object answer(InvocationOnMock invocation)
                    throws Throwable {
                S3Object s3Object = new S3Object();
                s3Object.setObjectContent(new ByteArrayInputStream("2015-01-01-00-00-00".getBytes(Charsets.UTF_8)));
                return s3Object;
            }
        });

        when(s3.listObjects(argThat(listObjectRequest("stash-bucket", "stash/test/2015-01-01-00-00-00/test~table/", null))))
                .thenAnswer(objectListingAnswer(null, "test~table-split0.gz", "test~table-split1.gz", "test~table-split2.gz"));

        StandardStashReader reader = new StandardStashReader(URI.create("s3://stash-bucket/stash/test"), s3, Duration.ZERO);
        StashTableMetadata tableMetadata = reader.getTableMetadata("test:table");

        assertEquals(tableMetadata.getBucket(), "stash-bucket");
        assertEquals(tableMetadata.getPrefix(), "stash/test/2015-01-01-00-00-00/test~table/");
        assertEquals(tableMetadata.getTableName(), "test:table");
        assertEquals(tableMetadata.getSize(), 300);

        List<StashFileMetadata> files = tableMetadata.getFiles();
        assertEquals(3, files.size());
        for (int i =0; i < 3; i++) {
            StashFileMetadata fileMetadata = files.get(i);
            assertEquals(fileMetadata.getBucket(), "stash-bucket");
            assertEquals(fileMetadata.getKey(),
                    String.format("stash/test/2015-01-01-00-00-00/test~table/test~table-split%d.gz", i));
            assertEquals(fileMetadata.getSize(), 100);
        }
    }

    @Test
    public void testGetSplits() throws Exception {
        AmazonS3 s3 = mock(AmazonS3.class);
        when(s3.getObject(argThat(getsObject("stash-bucket", "stash/test/_LATEST")))).thenAnswer(new Answer<S3Object>() {
            @Override
            public S3Object answer(InvocationOnMock invocation)
                    throws Throwable {
                S3Object s3Object = new S3Object();
                s3Object.setObjectContent(new ByteArrayInputStream("2015-01-01-00-00-00".getBytes(Charsets.UTF_8)));
                return s3Object;
            }
        });

        when(s3.listObjects(argThat(listObjectRequest("stash-bucket", "stash/test/2015-01-01-00-00-00/test~table/", null))))
                .thenAnswer(objectListingAnswer("marker1", "split0.gz", "split1.gz"));
        when(s3.listObjects(argThat(listObjectRequest("stash-bucket", "stash/test/2015-01-01-00-00-00/test~table/", "marker1"))))
                .thenAnswer(objectListingAnswer(null, "split2.gz", "split3.gz"));

        StandardStashReader reader = new StandardStashReader(URI.create("s3://stash-bucket/stash/test"), s3, Duration.ZERO);
        List<StashSplit> splits = reader.getSplits("test:table");
        assertEquals(splits.size(), 4);

        for (int i=0; i < 4; i++) {
            StashSplit split = splits.get(i);
            assertEquals(split.getTable(), "test:table");
            assertEquals(split.getFile(), "split" + i + ".gz");
            assertEquals(split.getKey(), "2015-01-01-00-00-00/test~table/split" + i + ".gz");
        }
    }

    @Test
    public void getSplit() throws Exception {
        // Intentionally create the gzip file as concatenated gzip files to verify the Java bug involving reading
        // concatenated gzip files is resolved.

        List<Map<String, Object>> expected = Lists.newArrayListWithCapacity(4);
        ByteArrayOutputStream splitOut = new ByteArrayOutputStream();

        for (int p=0; p < 2; p++) {
            for (int i=0; i < 2; i++) {
                ByteArrayOutputStream partialOut = new ByteArrayOutputStream();
                try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(partialOut)))) {
                    Map<String, Object> value = ImmutableMap.<String, Object>builder()
                            .put("~id", "row" + (p * 2 + i))
                            .put("~table", "test:table")
                            .put("~version", 1)
                            .put("~signature", "3a0da59fabf298d389b7b0b59728e887")
                            .put("~lastUpdateAt", "2014-08-28T21:24:36.440Z")
                            .put("~firstUpdateAt", "2014-06-07T09:51:40.077Z")
                            .build();

                    String json = JsonHelper.asJson(value);
                    out.write(json);
                    out.write("\n");

                    expected.add(value);
                }
                partialOut.writeTo(splitOut);
            }
        }

        splitOut.close();

        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(new ByteArrayInputStream(splitOut.toByteArray(), 0, splitOut.size()));
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(splitOut.size());
        s3Object.setObjectMetadata(objectMetadata);

        AmazonS3 s3 = mock(AmazonS3.class);
        when(s3.getObject(argThat(getsObject("stash-bucket", "stash/test/2015-01-01-00-00-00/test-table/split0.gz"))))
                .thenReturn(s3Object);

        StashSplit stashSplit = new StashSplit("test:table", "2015-01-01-00-00-00/test-table/split0.gz", splitOut.size());

        StandardStashReader reader = new StandardStashReader(URI.create("s3://stash-bucket/stash/test"), s3, Duration.ZERO);
        StashRowIterator contentIter = reader.getSplit(stashSplit);

        List<Map<String, Object>> content = ImmutableList.copyOf(contentIter);
        assertEquals(content, expected);
    }

    @Test
    public void testLockedView() throws Exception {
        AmazonS3 s3 = mock(AmazonS3.class);
        when(s3.getObject(argThat(getsObject("stash-bucket", "stash/test/_LATEST")))).thenAnswer(new Answer<S3Object>() {
            @Override
            public S3Object answer(InvocationOnMock invocation)
                    throws Throwable {
                S3Object s3Object = new S3Object();
                s3Object.setObjectContent(new ByteArrayInputStream("2015-01-02-00-00-00".getBytes(Charsets.UTF_8)));
                return s3Object;
            }
        });

        ObjectListing listing = new ObjectListing();
        listing.setCommonPrefixes(ImmutableList.of("stash/test/2015-01-01-00-00-00/table0101/"));
        listing.setTruncated(false);
        when(s3.listObjects(argThat(listObjectRequest("stash-bucket", "stash/test/2015-01-01-00-00-00/", null))))
                .thenReturn(listing);

        listing = new ObjectListing();
        listing.setCommonPrefixes(ImmutableList.of("stash/test/2015-01-02-00-00-00/table0102/"));
        listing.setTruncated(false);
        when(s3.listObjects(argThat(listObjectRequest("stash-bucket", "stash/test/2015-01-02-00-00-00/", null))))
                .thenReturn(listing);

        StandardStashReader reader = new StandardStashReader(URI.create("s3://stash-bucket/stash/test"), s3, Duration.ZERO);
        assertEquals(reader.getLatest(), "2015-01-02-00-00-00");

        // Create a locked view
        StashReader lockedView = reader.getLockedView();

        // Lock the original reader to the previous day
        reader.lockToStashCreatedAt(new ISO8601DateFormat().parse("2015-01-01T00:00:00Z"));

        Function<StashTable, String> toTableName = new Function<StashTable, String>() {
            @Override
            public String apply(StashTable stashTable) {
                return stashTable.getTableName();
            }
        };

        List<String> readerTables = ImmutableList.copyOf(Iterators.transform(reader.listTables(), toTableName));
        List<String> lockedViewTables = ImmutableList.copyOf(Iterators.transform(lockedView.listTables(), toTableName));

        assertEquals(readerTables, ImmutableList.of("table0101"));
        assertEquals(lockedViewTables, ImmutableList.of("table0102"));
    }

    private Matcher<GetObjectRequest> getsObject(final String bucket, final String key) {
        return new BaseMatcher<GetObjectRequest>() {
            @Override
            public boolean matches(Object o) {
                GetObjectRequest request = (GetObjectRequest) o;
                return request != null && request.getBucketName().equals(bucket) && request.getKey().equals(key);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("gets object s3://").appendText(bucket).appendText("/").appendText(key);
            }
        };
    }

    private Matcher<ListObjectsRequest> listObjectRequest(final String bucket, final String prefix, @Nullable final String marker) {
        return new BaseMatcher<ListObjectsRequest>() {
            @Override
            public boolean matches(Object item) {
                ListObjectsRequest request = (ListObjectsRequest) item;
                return request != null &&
                        request.getBucketName().equals(bucket) &&
                        request.getPrefix().equals(prefix) &&
                        Objects.equals(request.getMarker(), marker);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("ListObjectRequest[s3://").appendText(bucket).appendText("/").appendText(prefix);
                if (marker != null) {
                    description.appendText(", marker=").appendText(marker);
                }
                description.appendText("]");
            }
        };
    }

    private Answer<ObjectListing> objectListingAnswer(@Nullable final String marker, final String... fileNames) {
        return new Answer<ObjectListing>() {
            @Override
            public ObjectListing answer(InvocationOnMock invocation)
                    throws Throwable {
                ListObjectsRequest request = (ListObjectsRequest) invocation.getArguments()[0];

                ObjectListing objectListing = new ObjectListing();
                objectListing.setBucketName(request.getBucketName());
                objectListing.setPrefix(request.getPrefix());

                objectListing.setTruncated(marker != null);
                objectListing.setNextMarker(marker);

                for (String fileName : fileNames) {
                    S3ObjectSummary objectSummary = new S3ObjectSummary();
                    objectSummary.setKey(request.getPrefix() + fileName);
                    objectSummary.setSize(100);
                    objectListing.getObjectSummaries().add(objectSummary);
                }

                return objectListing;
            }
        };
    }

    /**
     * Note:  This test depends on the default character set to be US-ASCII
     */
    @Test
    public void testUTF8ClientDecoding() throws Exception {
        if (!Charset.defaultCharset().equals(Charsets.US_ASCII)) {
            throw new SkipException("testUTF8ClientDecoding() requires default charset to be US-ASCII");
        }

        String utf8Text = "ชิงรางวัลกันจ้า";
        ByteArrayOutputStream splitOut = new ByteArrayOutputStream();

        try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(splitOut), Charsets.UTF_8))) {
            Map<String, Object> value = ImmutableMap.<String, Object>builder()
                    .put("~id", "1")
                    .put("~table", "test:table")
                    .put("~version", 1)
                    .put("~signature", "3a0da59fabf298d389b7b0b59728e887")
                    .put("~lastUpdateAt", "2014-08-28T21:24:36.440Z")
                    .put("~firstUpdateAt", "2014-06-07T09:51:40.077Z")
                    .put("text", utf8Text)
                    .build();

            String json = JsonHelper.asJson(value);
            out.write(json);
            out.write("\n");
        }

        splitOut.close();

        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(new ByteArrayInputStream(splitOut.toByteArray(), 0, splitOut.size()));
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(splitOut.size());
        s3Object.setObjectMetadata(objectMetadata);

        AmazonS3 s3 = mock(AmazonS3.class);
        when(s3.getObject(argThat(getsObject("stash-bucket", "stash/test/2015-01-01-00-00-00/test-table/split0.gz"))))
                .thenReturn(s3Object);

        StashSplit stashSplit = new StashSplit("test:table", "2015-01-01-00-00-00/test-table/split0.gz", splitOut.size());

        StandardStashReader reader = new StandardStashReader(URI.create("s3://stash-bucket/stash/test"), s3, Duration.ZERO);
        try (StashRowIterator rowIter = reader.getSplit(stashSplit)) {
            assertTrue(rowIter.hasNext());
            Map<String, Object> row = rowIter.next();
            assertEquals(row.get("text"), utf8Text);
        }
    }
}

