package com.bazaarvoice.emodb.web.scanner;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.json.ISO8601DateFormat;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.plugin.stash.StashMetadata;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.bazaarvoice.emodb.queue.core.ByteBufferInputStream;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.compactioncontrol.InMemoryCompactionControlSource;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.MultiTableScanOptions;
import com.bazaarvoice.emodb.sor.db.MultiTableScanResult;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.db.ScanRangeSplits;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.web.scanner.control.DistributedScanRangeMonitor;
import com.bazaarvoice.emodb.web.scanner.control.InMemoryScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.control.LocalScanUploadMonitor;
import com.bazaarvoice.emodb.web.scanner.control.ScanRangeComplete;
import com.bazaarvoice.emodb.web.scanner.control.ScanRangeTask;
import com.bazaarvoice.emodb.web.scanner.control.ScanTableSetManager;
import com.bazaarvoice.emodb.web.scanner.control.ScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.notifications.ScanCountListener;
import com.bazaarvoice.emodb.web.scanner.rangescan.LocalRangeScanUploader;
import com.bazaarvoice.emodb.web.scanner.rangescan.RangeScanUploader;
import com.bazaarvoice.emodb.web.scanner.rangescan.RangeScanUploaderResult;
import com.bazaarvoice.emodb.web.scanner.scanstatus.DataStoreScanStatusDAO;
import com.bazaarvoice.emodb.web.scanner.scanstatus.InMemoryScanStatusDAO;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanRangeStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatusDAO;
import com.bazaarvoice.emodb.web.scanner.writer.DiscardingScanWriter;
import com.bazaarvoice.emodb.web.scanner.writer.S3ScanWriter;
import com.bazaarvoice.emodb.web.scanner.writer.ScanWriter;
import com.bazaarvoice.emodb.web.scanner.writer.ScanWriterFactory;
import com.bazaarvoice.emodb.web.scanner.writer.ScanWriterGenerator;
import com.bazaarvoice.emodb.web.scanner.writer.TransferKey;
import com.bazaarvoice.emodb.web.scanner.writer.TransferStatus;
import com.bazaarvoice.emodb.web.scanner.writer.WaitForAllTransfersCompleteResult;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.astyanax.connectionpool.exceptions.TokenRangeOfflineException;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static java.lang.String.format;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit test which validates the scan upload operation.
 */
public class ScanUploaderTest {

    private ScheduledExecutorService _service;

    @BeforeMethod
    public void setUp() {
        _service = Executors.newScheduledThreadPool(3);
    }

    @AfterMethod
    public void tearDown() {
        _service.shutdown();
    }

    @Test
    public void testScheduling()
            throws Exception {
        ScanWorkflow scanWorkflow = mock(ScanWorkflow.class);
        when(scanWorkflow.addScanRangeTask(anyString(), anyInt(), anyString(), any(ScanRange.class))).thenAnswer(new Answer<ScanRangeTask>() {
            @Override
            public ScanRangeTask answer(InvocationOnMock invocation)
                    throws Throwable {
                ScanRangeTask task = mock(ScanRangeTask.class);
                when(task.toString()).thenReturn(String.format("%s/%s/%s", invocation.getArguments()));
                return task;
            }
        });

        String id = "id";
        ScanStatusDAO scanStatusDAO = new InMemoryScanStatusDAO();

        LocalScanUploadMonitor monitor = new LocalScanUploadMonitor(scanWorkflow, scanStatusDAO,
                mock(ScanTableSetManager.class), mock(ScanWriterGenerator.class), mock(StashStateListener.class),
                mock(ScanCountListener.class), mock(DataTools.class), new InMemoryCompactionControlSource(), mock(DataCenters.class));
        monitor.setExecutorService(mock(ScheduledExecutorService.class));

        ScanOptions options = new ScanOptions(ImmutableList.of("p0", "p1"))
                .setScanByAZ(true)
                .setMaxConcurrentSubRangeScans(2);

        List<ScanRangeStatus> statuses = Lists.newArrayList();
        com.google.common.collect.Table<String, ScanRange, Integer> taskForRange = HashBasedTable.create();

        int taskId = 0;
        for (int b = 0; b < 3; b++) {
            Optional<Integer> blockedByBatchId = b == 0 ? Optional.<Integer>absent() : Optional.of(b - 1);

            for (int p = 0; p < 2; p++) {
                String placement = "p" + p;
                // Intentionally insert scan ranges in a different order than they should be returned
                for (int r = 3; r >= 0; r--) {
                    ScanRange scanRange = ScanRange.create(asByteBuffer(0, b * 4L + r), asByteBuffer(0, (b * 4L + r + 1)));
                    statuses.add(new ScanRangeStatus(taskId, placement, scanRange, b, blockedByBatchId, Optional.of(b * 2 + p)));
                    taskForRange.put(placement, scanRange, taskId);
                    taskId++;
                }
            }
        }

        ScanStatus scanStatus = new ScanStatus(id, options, false, new Date(), statuses,
                Lists.<ScanRangeStatus>newArrayList(), Lists.<ScanRangeStatus>newArrayList());

        scanStatusDAO.updateScanStatus(scanStatus);

        monitor.refreshScan(id);
        verify(scanWorkflow).addScanRangeTask(eq(id), anyInt(), eq("p0"), eq(ScanRange.create(asByteBuffer(0, 0L), asByteBuffer(0, 1L))));
        verify(scanWorkflow).addScanRangeTask(eq(id), anyInt(), eq("p0"), eq(ScanRange.create(asByteBuffer(0, 1L), asByteBuffer(0, 2L))));
        verify(scanWorkflow).addScanRangeTask(eq(id), anyInt(), eq("p1"), eq(ScanRange.create(asByteBuffer(0, 0L), asByteBuffer(0, 1L))));
        verify(scanWorkflow).addScanRangeTask(eq(id), anyInt(), eq("p1"), eq(ScanRange.create(asByteBuffer(0, 1L), asByteBuffer(0, 2L))));
        verifyNoMoreInteractions(scanWorkflow);

        Date now = new Date();

        for (String p : ImmutableList.of("p0", "p1")) {
            scanStatusDAO.setScanRangeTaskActive(id, taskForRange.get(p, ScanRange.create(asByteBuffer(0, 0L), asByteBuffer(0, 1L))), now);
            scanStatusDAO.setScanRangeTaskComplete(id, taskForRange.get(p, ScanRange.create(asByteBuffer(0, 0L), asByteBuffer(0, 1L))), now);
            monitor.refreshScan(id);
            verify(scanWorkflow).addScanRangeTask(eq(id), anyInt(), eq(p), eq(ScanRange.create(asByteBuffer(0, 2L), asByteBuffer(0, 3L))));
            verifyNoMoreInteractions(scanWorkflow);

            scanStatusDAO.setScanRangeTaskActive(id, taskForRange.get(p, ScanRange.create(asByteBuffer(0, 1L), asByteBuffer(0, 2L))), now);
            scanStatusDAO.setScanRangeTaskComplete(id, taskForRange.get(p, ScanRange.create(asByteBuffer(0, 1L), asByteBuffer(0, 2L))), now);
            monitor.refreshScan(id);
            verify(scanWorkflow).addScanRangeTask(eq(id), anyInt(), eq(p), eq(ScanRange.create(asByteBuffer(0, 3L), asByteBuffer(0, 4L))));
            verifyNoMoreInteractions(scanWorkflow);
        }

        for (String p : ImmutableList.of("p0", "p1")) {
            scanStatusDAO.setScanRangeTaskActive(id, taskForRange.get(p, ScanRange.create(asByteBuffer(0, 2L), asByteBuffer(0, 3L))), now);
            scanStatusDAO.setScanRangeTaskComplete(id, taskForRange.get(p, ScanRange.create(asByteBuffer(0, 2L), asByteBuffer(0, 3L))), now);
            monitor.refreshScan(id);
            verifyNoMoreInteractions(scanWorkflow);
        }

        scanStatusDAO.setScanRangeTaskActive(id, taskForRange.get("p0", ScanRange.create(asByteBuffer(0, 3L), asByteBuffer(0, 4L))), now);
        scanStatusDAO.setScanRangeTaskComplete(id, taskForRange.get("p0", ScanRange.create(asByteBuffer(0, 3L), asByteBuffer(0, 4L))), now);
        monitor.refreshScan(id);
        verifyNoMoreInteractions(scanWorkflow);

        scanStatusDAO.setScanRangeTaskActive(id, taskForRange.get("p1", ScanRange.create(asByteBuffer(0, 3L), asByteBuffer(0, 4L))), now);
        scanStatusDAO.setScanRangeTaskComplete(id, taskForRange.get("p1", ScanRange.create(asByteBuffer(0, 3L), asByteBuffer(0, 4L))), now);
        monitor.refreshScan(id);
        verify(scanWorkflow).addScanRangeTask(eq(id), anyInt(), eq("p0"), eq(ScanRange.create(asByteBuffer(0, 4L), asByteBuffer(0, 5L))));
        verify(scanWorkflow).addScanRangeTask(eq(id), anyInt(), eq("p0"), eq(ScanRange.create(asByteBuffer(0, 5L), asByteBuffer(0, 6L))));
        verify(scanWorkflow).addScanRangeTask(eq(id), anyInt(), eq("p1"), eq(ScanRange.create(asByteBuffer(0, 4L), asByteBuffer(0, 5L))));
        verify(scanWorkflow).addScanRangeTask(eq(id), anyInt(), eq("p1"), eq(ScanRange.create(asByteBuffer(0, 5L), asByteBuffer(0, 6L))));
        verifyNoMoreInteractions(scanWorkflow);

        // Could go farther but at this point we've tried all possible variations with this configuration
    }

    private ByteBuffer asByteBuffer(int shardId, long tableUuid) {
        return AstyanaxStorage.getRowKeyRaw(shardId, tableUuid, "");
    }

    @Test
    public void testScanUploader()
            throws Exception {
        // Mock out a DataTools that will return scan results spread consistently across 8 shards
        DataTools dataTools = mock(DataTools.class);
        when(dataTools.getTablePlacements(true, true)).thenReturn(ImmutableList.of("placement1"));
        when(dataTools.getScanRangeSplits(eq("placement1"), anyInt(), eq(Optional.<ScanRange>absent()))).thenReturn(
                ScanRangeSplits.builder()
                        .addScanRange("dummy", "dummy", ScanRange.all())
                        .build());
        when(dataTools.multiTableScan(any(MultiTableScanOptions.class), any(TableSet.class), any(LimitCounter.class), any(ReadConsistency.class), any(DateTime.class)))
                .thenReturn(createMockScanResults());
        when(dataTools.toContent(any(MultiTableScanResult.class), any(ReadConsistency.class), eq(false)))
                .thenAnswer(new Answer<Map<String, Object>>() {
                    @Override
                    public Map<String, Object> answer(InvocationOnMock invocation)
                            throws Throwable {
                        MultiTableScanResult result = (MultiTableScanResult) invocation.getArguments()[0];
                        return ImmutableMap.<String, Object>builder()
                                .put(Intrinsic.ID, result.getRecord().getKey().getKey())
                                .put(Intrinsic.TABLE, tableName(result.getTableUuid()))
                                .put(Intrinsic.DELETED, Boolean.FALSE)
                                .put(Intrinsic.VERSION, 1)
                                .build();
                    }
                });

        // Simulate transfers to S3 and store the actual transfer contents in the following table,
        // keyed by parent directory and file name
        final HashBasedTable<String, String, ByteBuffer> s3Files = HashBasedTable.create();

        final AmazonS3 amazonS3 = mock(AmazonS3.class);
        when(amazonS3.putObject(any(PutObjectRequest.class))).thenAnswer(
                new Answer<PutObjectResult>() {
                    @Override
                    public PutObjectResult answer(InvocationOnMock invocation)
                            throws Throwable {
                        PutObjectRequest request = (PutObjectRequest) invocation.getArguments()[0];
                        String bucket = request.getBucketName();
                        String key = request.getKey();
                        if (request.getFile() != null) {
                            return mockUploadS3File(bucket, key, Files.toByteArray(request.getFile()), s3Files);
                        }
                        return mockUploadS3File(bucket, key, ByteStreams.toByteArray(request.getInputStream()), s3Files);
                    }
                }
        );

        AmazonS3Exception notFoundException = new AmazonS3Exception("not found");
        notFoundException.setStatusCode(404);
        when(amazonS3.getObjectMetadata("testbucket", "test/path/_SUCCESS"))
                .thenThrow(notFoundException)
                .thenReturn(new ObjectMetadata());

        final MetricRegistry metricRegistry = new MetricRegistry();
        ScanWriterFactory scanWriterFactory = mock(ScanWriterFactory.class);
        when(scanWriterFactory.createS3ScanWriter(anyInt(), any(URI.class), any(Optional.class))).thenAnswer(
                new Answer<S3ScanWriter>() {
                    @Override
                    public S3ScanWriter answer(InvocationOnMock invocation)
                            throws Throwable {
                        int taskId = (Integer) invocation.getArguments()[0];
                        URI uri = (URI) invocation.getArguments()[1];
                        Optional<Integer> maxOpenShards = (Optional<Integer>) invocation.getArguments()[2];
                        return new S3ScanWriter(taskId, uri, maxOpenShards, metricRegistry, amazonS3, _service);
                    }
                }
        );
        ScanWriterGenerator scanWriterGenerator = new ScanWriterGenerator(scanWriterFactory);

        TableSet tableSet = mock(TableSet.class);
        StashStateListener stashStateListener = mock(StashStateListener.class);
        ScanCountListener scanCountListener = mock(ScanCountListener.class);
        DataCenters dataCenters = mock(DataCenters.class);
        DataCenter dataCenter1 = mockDataCenter("us-east", "http://emodb.cert.us-east-1.nexus.bazaarvoice.com:8081", "http://emodb.cert.us-east-1.nexus.bazaarvoice.com:8080");
        when(dataCenters.getSelf()).thenReturn(dataCenter1);
        when(dataCenters.getAll()).thenReturn(ImmutableList.of(dataCenter1));

        ScanWorkflow scanWorkflow = new InMemoryScanWorkflow();
        ScanStatusDAO scanStatusDAO = new InMemoryScanStatusDAO();
        CompactionControlSource compactionControlSource = new InMemoryCompactionControlSource();
        // Create the instance and run the upload
        ScanUploader scanUploader = new ScanUploader(dataTools, scanWorkflow, scanStatusDAO, stashStateListener, compactionControlSource, dataCenters);
        scanUploader.scanAndUpload("test1",
                new ScanOptions("placement1").addDestination(ScanDestination.to(new URI("s3://testbucket/test/path"))));
        LocalScanUploadMonitor monitor = new LocalScanUploadMonitor(scanWorkflow, scanStatusDAO, mock(ScanTableSetManager.class),
                scanWriterGenerator, stashStateListener, scanCountListener, dataTools, compactionControlSource, dataCenters);
        monitor.setExecutorService(mock(ScheduledExecutorService.class));


        monitor.refreshScan("test1");

        // Verify the scan status was recorded and is consistent
        ScanStatus scanStatus = scanStatusDAO.getScanStatus("test1");
        Date startTime = scanStatus.getStartTime();
        assertEquals(scanStatus.getPendingScanRanges().size(), 1);
        assertTrue(scanStatus.getActiveScanRanges().isEmpty());
        assertTrue(scanStatus.getCompleteScanRanges().isEmpty());
        // Verify notification of the scan starting was sent
        verify(stashStateListener).stashStarted(argThat(matchesScan("test1")));

        // Simulate an asynchronous process claiming and scanning the single scan range created
        List<ScanRangeTask> tasks = scanWorkflow.claimScanRangeTasks(Integer.MAX_VALUE, Duration.standardMinutes(1));
        assertEquals(tasks.size(), 1);
        ScanRangeTask task = Iterables.getOnlyElement(tasks);

        assertEquals(task.getId(), 0);
        assertEquals(task.getScanId(), "test1");
        assertEquals(task.getRange(), ScanRange.all());

        // Mark that the task is active
        scanStatusDAO.setScanRangeTaskActive("test1", task.getId(), new Date());

        // Scan and upload the range
        LocalRangeScanUploader uploader = new LocalRangeScanUploader(
                dataTools, scanWriterGenerator, compactionControlSource, mock(LifeCycleRegistry.class), metricRegistry, 2, 1000, Duration.standardMinutes(1),
                Duration.standardMinutes(5));
        uploader.start();
        try {
            uploader.scanAndUpload(task.getId(), scanStatus.getOptions(), "placement1", task.getRange(), tableSet, new Date());
        } finally {
            uploader.stop();
        }

        // 20 tables were written, table00 to table19
        for (int table = 0; table < 20; table++) {
            // Because of row buffering the actual number of files will vary.  Verify that each expected row
            // was written exactly once across all files found.
            Set<String> expectedIds = Sets.newHashSet();
            for (int shard = 0; shard < 8; shard++) {
                for (int row = 0; row < 20; row++) {
                    expectedIds.add(key(shard, row));
                }
            }

            Collection<ByteBuffer> files = s3Files.row("testbucket/test/path/" + tableName(table)).values();
            for (ByteBuffer contents : files) {
                byte[] unzippedContents = ByteStreams.toByteArray(new GZIPInputStream(new ByteBufferInputStream(contents)));
                String jsonContents = new String(unzippedContents, Charsets.UTF_8);
                String[] jsonLines = jsonContents.split("\n");

                for (String json : jsonLines) {
                    Map<String, Object> map = JsonHelper.fromJson(json, Map.class);
                    assertTrue(expectedIds.remove((String) map.get(Intrinsic.ID)));
                    assertEquals(map.get(Intrinsic.TABLE), tableName(table));
                }
            }

            assertTrue(expectedIds.isEmpty(), format("Records not found in Stash: %s", expectedIds));
        }

        long beforeCompleteTs = System.currentTimeMillis();

        // Finish the scan
        scanStatusDAO.setScanRangeTaskComplete("test1", task.getId(), new Date());
        monitor.refreshScan("test1");
        scanStatus = scanStatusDAO.getScanStatus("test1");

        // Scan complete time was set
        assertNotNull(scanStatus.getCompleteTime());
        // Notification of the scan completion was sent
        verify(stashStateListener).stashCompleted(argThat(matchesScan("test1")), eq(scanStatus.getCompleteTime()));

        // Success file was written
        ByteBuffer content = s3Files.get("testbucket/test/path", "_SUCCESS");
        assertNotNull(content);
        // First line is the start timestamp, second line is the end timestamp, third line is the name of the task
        List<String> lines = CharStreams.readLines(new InputStreamReader(new ByteBufferInputStream(content)));
        assertEquals(lines.size(), 3);
        assertEquals(lines.get(0), new ISO8601DateFormat().format(startTime));
        assertTrue(Range.closed(beforeCompleteTs, System.currentTimeMillis()).contains(new ISO8601DateFormat().parse(lines.get(1)).getTime()));
        assertEquals(lines.get(2), "test1");

        // Latest file was written and contains the name of the path, "path"
        content = s3Files.get("testbucket/test", "_LATEST");
        assertNotNull(content);
        assertEquals(CharStreams.toString(new InputStreamReader(new ByteBufferInputStream(content))), "path");

        // Verify that resubmitting the refresh scan does not spawn rewrites of the success or latest files
        monitor.refreshScan("test1");
        verify(amazonS3, times(1)).putObject(argThat(putsObject("testbucket", "test/path/_SUCCESS")));
        verify(amazonS3, times(1)).putObject(argThat(putsObject("testbucket", "test/_LATEST")));
    }

    private PutObjectResult mockUploadS3File(String bucket, String key, byte[] contents, HashBasedTable<String, String, ByteBuffer> s3FileTable) {
        // Place the contents in the s3 file table keyed by the file's parent directory and file name
        int idx = key.lastIndexOf('/');
        String parentDir = key.substring(0, idx);
        String fileName = key.substring(idx + 1);
        // HashBasedTable is not thread-safe if multiple threads try to write to the same directory concurrently
        synchronized (s3FileTable) {
            s3FileTable.put(format("%s/%s", bucket, parentDir), fileName, ByteBuffer.wrap(contents));
        }

        PutObjectResult result = new PutObjectResult();
        result.setETag("etag");
        return result;
    }

    public Matcher<PutObjectRequest> putsObject(final String bucket, final String key) {
        return new BaseMatcher<PutObjectRequest>() {
            @Override
            public boolean matches(Object item) {
                PutObjectRequest request = (PutObjectRequest) item;
                return request != null && bucket.equals(request.getBucketName()) && key.equals(request.getKey());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("puts s3://").appendText(bucket).appendText("/").appendText(key);
            }
        };
    }

    /**
     * Simulates creating 20 tables, each with 160 rows spread evenly across 8 shards.
     */
    private Iterator<MultiTableScanResult> createMockScanResults() {
        return new AbstractIterator<MultiTableScanResult>() {
            private int shard = 0;
            private long tableUuid = 0;
            private int row = -1;

            @Override
            protected MultiTableScanResult computeNext() {
                if (++row == 20) {
                    // Start a new table
                    row = 0;
                    if (++tableUuid == 20) {
                        // Start a new shard
                        tableUuid = 0;
                        if (++shard == 8) {
                            // All 8 shards written
                            return endOfData();
                        }
                    }
                }

                String keyString = key(shard, row);
                Record record = mock(Record.class);
                Key key = mock(Key.class);
                when(key.getKey()).thenReturn(keyString);
                Table table = mock(Table.class);
                when(table.getName()).thenReturn(tableName(tableUuid));
                when(key.getTable()).thenReturn(table);
                when(record.getKey()).thenReturn(key);
                when(table.getOptions()).thenReturn(new TableOptionsBuilder().setPlacement("placement1").build());
                return new MultiTableScanResult(AstyanaxStorage.getRowKeyRaw(shard, tableUuid, keyString), shard, tableUuid, false, record);
            }
        };
    }

    private String tableName(long uuid) {
        return format("table%02d", uuid);
    }

    private String key(int shard, int row) {
        return format("%02d%02d", shard, row);
    }

    @Test
    public void testScanFailureRecovery()
            throws Exception {

        // Create a simple scan with a single range
        String id = "id";
        ScanOptions options = new ScanOptions(ImmutableList.of("p0"));
        ScanStatus scanStatus = new ScanStatus(id, options, false, new Date(),
                ImmutableList.of(new ScanRangeStatus(123, "p0", ScanRange.all(), 0, Optional.<Integer>absent(), Optional.<Integer>absent())),
                Lists.<ScanRangeStatus>newArrayList(), Lists.<ScanRangeStatus>newArrayList());

        // Mock needs to return an object, actual value isn't important for this test
        ScanTableSetManager scanTableSetManager = mock(ScanTableSetManager.class);
        when(scanTableSetManager.getTableSetForScan(id)).thenReturn(mock(TableSet.class));

        InMemoryScanWorkflow scanWorkflow = new InMemoryScanWorkflow();
        ScanStatusDAO scanStatusDAO = new DataStoreScanStatusDAO(new InMemoryDataStore(new MetricRegistry()), "scan_table", "app_global:sys");
        LocalScanUploadMonitor monitor = new LocalScanUploadMonitor(scanWorkflow, scanStatusDAO, scanTableSetManager,
                mock(ScanWriterGenerator.class), mock(StashStateListener.class), mock(ScanCountListener.class),
                mock(DataTools.class), new InMemoryCompactionControlSource(), mock(DataCenters.class));
        monitor.setExecutorService(mock(ScheduledExecutorService.class));

        // Store the scan
        scanStatusDAO.updateScanStatus(scanStatus);

        // Refresh the state for the scan
        monitor.refreshScan(id);

        // Verify the scan was queued as a pending task
        List<ScanRangeTask> pendingTasks = scanWorkflow.peekAllPendingTasks();
        assertEquals(pendingTasks.size(), 1);
        assertEquals(pendingTasks.get(0).getScanId(), id);
        assertEquals(pendingTasks.get(0).getId(), 123);

        // Create a RangeScanUploader which will raise an uncaught exception when the scan is attempted.
        RangeScanUploader rangeScanUploader = mock(RangeScanUploader.class);
        doThrow(new RuntimeException("Mock error reading from Cassandra"))
                .when(rangeScanUploader).scanAndUpload(eq(123), eq(options), eq("p0"), eq(ScanRange.all()), any(TableSet.class), any(Date.class));

        // Set up the distributed scan range monitor
        DistributedScanRangeMonitor distributedScanRangeMonitor = new DistributedScanRangeMonitor(
                scanWorkflow, scanStatusDAO, rangeScanUploader, scanTableSetManager, 1, mock(LifeCycleRegistry.class));

        // Give it an executor service that will run the scan synchronously in the current thread
        distributedScanRangeMonitor.setExecutorServices(MoreExecutors.sameThreadExecutor(), mock(ScheduledExecutorService.class));
        distributedScanRangeMonitor.startScansIfAvailable();

        // Verify the task is complete and there are no pending tasks
        List<ScanRangeComplete> completeTasks = scanWorkflow.peekAllCompletedTasks();
        assertEquals(completeTasks.size(), 1);
        assertEquals(completeTasks.get(0).getScanId(), id);
        assertEquals(scanWorkflow.peekAllPendingTasks().size(), 0);

        // Refresh the state again
        monitor.refreshScan(id);

        // Verify the task was resubmitted
        pendingTasks = scanWorkflow.peekAllPendingTasks();
        assertEquals(pendingTasks.size(), 1);
        assertEquals(pendingTasks.get(0).getScanId(), id);
        assertEquals(pendingTasks.get(0).getId(), 123);

        verify(rangeScanUploader).scanAndUpload(eq(123), eq(options), eq("p0"), eq(ScanRange.all()), any(TableSet.class), any(Date.class));
        verifyNoMoreInteractions(rangeScanUploader);
    }

    @Test
    public void testScanResultStorageFailure()
            throws Exception {
        // Create a simple scan with a single range
        String id = "id";
        String placement = "p0";
        ScanOptions options = new ScanOptions(ImmutableList.of(placement));
        ScanStatus scanStatus = new ScanStatus(id, options, false, new Date(),
                ImmutableList.of(new ScanRangeStatus(123, placement, ScanRange.all(), 0, Optional.<Integer>absent(), Optional.<Integer>absent())),
                Lists.<ScanRangeStatus>newArrayList(), Lists.<ScanRangeStatus>newArrayList());

        ScanStatusDAO scanStatusDAO = mock(ScanStatusDAO.class);
        when(scanStatusDAO.getScanStatus(id)).thenReturn(scanStatus);
        // Simulate the token range being offline when the results are persisted
        doThrow(new RuntimeException(new TokenRangeOfflineException("token range offline")))
                .when(scanStatusDAO).setScanRangeTaskComplete(eq("id"), eq(123), any(Date.class));

        ScanRangeTask task = mock(ScanRangeTask.class);
        when(task.getScanId()).thenReturn(id);
        when(task.getId()).thenReturn(123);
        when(task.getPlacement()).thenReturn(placement);
        when(task.getRange()).thenReturn(ScanRange.all());

        ScanWorkflow scanWorkflow = mock(ScanWorkflow.class);
        when(scanWorkflow.claimScanRangeTasks(anyInt(), any(Duration.class))).thenReturn(
                ImmutableList.of(task), ImmutableList.<ScanRangeTask>of()
        );

        RangeScanUploader rangeScanUploader = mock(RangeScanUploader.class);
        when(rangeScanUploader.scanAndUpload(anyInt(), eq(options), eq(placement), eq(ScanRange.all()), any(TableSet.class), any(Date.class)))
                .thenReturn(RangeScanUploaderResult.success());
        ScanTableSetManager scanTableSetManager = mock(ScanTableSetManager.class);

        // Set up the distributed scan range monitor
        DistributedScanRangeMonitor distributedScanRangeMonitor = new DistributedScanRangeMonitor(
                scanWorkflow, scanStatusDAO, rangeScanUploader, scanTableSetManager, 1, mock(LifeCycleRegistry.class));

        // Give it an executor service that will run the scan synchronously in the current thread
        distributedScanRangeMonitor.setExecutorServices(MoreExecutors.sameThreadExecutor(), mock(ScheduledExecutorService.class));
        distributedScanRangeMonitor.startScansIfAvailable();

        // The scan range should never have been released
        verify(scanWorkflow, never()).releaseScanRangeTask(any(ScanRangeTask.class));

        verify(scanStatusDAO).getScanStatus(id);
        verify(scanStatusDAO).setScanRangeTaskActive(eq(id), eq(123), any(Date.class));
        verify(scanStatusDAO).setScanRangeTaskComplete(eq(id), eq(123), any(Date.class));
        verify(scanWorkflow, atLeastOnce()).claimScanRangeTasks(anyInt(), any(Duration.class));
        verify(scanWorkflow, atLeastOnce()).renewScanRangeTasks(anyCollection(), any(Duration.class));
        verifyNoMoreInteractions(scanStatusDAO, scanWorkflow);
    }

    @Test
    public void testWorkflowRecoveryForPartiallyCompleteScan()
            throws Exception {
        ScanOptions options = new ScanOptions(ImmutableList.of("p0"));
        List<ScanRangeStatus> completeTasks = ImmutableList.of(
                new ScanRangeStatus(0, "p0", ScanRange.create(ByteBuffer.wrap(new byte[] {0x00}), ByteBuffer.wrap(new byte[] {0x01})),
                        0, Optional.<Integer>absent(), Optional.<Integer>absent()),
                new ScanRangeStatus(1, "p0", ScanRange.create(ByteBuffer.wrap(new byte[] {0x02}), ByteBuffer.wrap(new byte[] {0x03})),
                        0, Optional.<Integer>absent(), Optional.<Integer>absent()));

        List<ScanRangeStatus> activeTasks = ImmutableList.of(
                new ScanRangeStatus(2, "p0", ScanRange.create(ByteBuffer.wrap(new byte[] {0x04}), ByteBuffer.wrap(new byte[] {0x05})),
                        1, Optional.<Integer>absent(), Optional.<Integer>absent()),
                new ScanRangeStatus(3, "p0", ScanRange.create(ByteBuffer.wrap(new byte[] {0x06}), ByteBuffer.wrap(new byte[] {0x07})),
                        1, Optional.<Integer>absent(), Optional.<Integer>absent()));

        List<ScanRangeStatus> pendingTasks = ImmutableList.of(
                new ScanRangeStatus(4, "p0", ScanRange.create(ByteBuffer.wrap(new byte[] {0x08}), ByteBuffer.wrap(new byte[] {0x09})),
                        2, Optional.<Integer>absent(), Optional.<Integer>absent()),
                new ScanRangeStatus(5, "p0", ScanRange.create(ByteBuffer.wrap(new byte[] {0x0a}), ByteBuffer.wrap(new byte[] {0x0b})),
                        2, Optional.<Integer>absent(), Optional.<Integer>absent()));


        for (ScanRangeStatus status : Iterables.concat(completeTasks, activeTasks)) {
            status.setScanQueuedTime(new Date());
            status.setScanStartTime(new Date());
        }
        for (ScanRangeStatus status : completeTasks) {
            status.setScanCompleteTime(new Date());
        }

        ScanStatus scanStatus = new ScanStatus("id", options, false, new Date(), pendingTasks, activeTasks, completeTasks);

        ScanWorkflow scanWorkflow = mock(ScanWorkflow.class);
        ScanStatusDAO scanStatusDAO = mock(ScanStatusDAO.class);
        when(scanStatusDAO.getScanStatus("id")).thenReturn(scanStatus);
        DataCenters dataCenters = mock(DataCenters.class);
        DataCenter dataCenter1 = mockDataCenter("us-east", "http://emodb.cert.us-east-1.nexus.bazaarvoice.com:8081", "http://emodb.cert.us-east-1.nexus.bazaarvoice.com:8080");
        when(dataCenters.getSelf()).thenReturn(dataCenter1);
        when(dataCenters.getAll()).thenReturn(ImmutableList.of(dataCenter1));

        ScanUploader scanUploader = new ScanUploader(mock(DataTools.class), scanWorkflow, scanStatusDAO, mock(StashStateListener.class), new InMemoryCompactionControlSource(), mock(DataCenters.class));
        scanUploader.resubmitWorkflowTasks("id");

        verify(scanStatusDAO).getScanStatus("id");
        verify(scanWorkflow).addScanRangeTask("id", 2, "p0", ScanRange.create(ByteBuffer.wrap(new byte[] {0x04}), ByteBuffer.wrap(new byte[] {0x05})));
        verify(scanWorkflow).addScanRangeTask("id", 3, "p0", ScanRange.create(ByteBuffer.wrap(new byte[] {0x06}), ByteBuffer.wrap(new byte[] {0x07})));
        verify(scanWorkflow).scanStatusUpdated("id");
        verifyNoMoreInteractions(scanStatusDAO, scanWorkflow);
    }

    @Test
    public void testWorkflowRecoveryForFullyCompleteScan()
            throws Exception {
        ScanOptions options = new ScanOptions(ImmutableList.of("p0"));
        ScanRangeStatus status = new ScanRangeStatus(0, "p0", ScanRange.all(), 0, Optional.<Integer>absent(), Optional.<Integer>absent());
        status.setScanQueuedTime(new Date());
        status.setScanCompleteTime(new Date());

        ScanStatus scanStatus = new ScanStatus("id", options, false, new Date(), ImmutableList.<ScanRangeStatus>of(),
                ImmutableList.<ScanRangeStatus>of(), ImmutableList.of(status), new Date());

        ScanWorkflow scanWorkflow = mock(ScanWorkflow.class);
        ScanStatusDAO scanStatusDAO = mock(ScanStatusDAO.class);
        when(scanStatusDAO.getScanStatus("id")).thenReturn(scanStatus);
        DataCenters dataCenters = mock(DataCenters.class);
        DataCenter dataCenter1 = mockDataCenter("us-east", "http://emodb.cert.us-east-1.nexus.bazaarvoice.com:8081", "http://emodb.cert.us-east-1.nexus.bazaarvoice.com:8080");
        when(dataCenters.getSelf()).thenReturn(dataCenter1);
        when(dataCenters.getAll()).thenReturn(ImmutableList.of(dataCenter1));

        ScanUploader scanUploader = new ScanUploader(mock(DataTools.class), scanWorkflow, scanStatusDAO, mock(StashStateListener.class), new InMemoryCompactionControlSource(), mock(DataCenters.class));
        scanUploader.resubmitWorkflowTasks("id");

        verify(scanStatusDAO).getScanStatus("id");
        verify(scanWorkflow, never()).addScanRangeTask(anyString(), anyInt(), anyString(), any(ScanRange.class));
        verify(scanWorkflow, never()).scanStatusUpdated(anyString());
        verifyNoMoreInteractions(scanStatusDAO, scanWorkflow);
    }

    private Matcher<StashMetadata> matchesScan(final String scanId) {
        return new BaseMatcher<StashMetadata>() {
            @Override
            public boolean matches(Object item) {
                return item != null && item instanceof StashMetadata && ((StashMetadata) item).getId().equals(scanId);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("stash info for ").appendText(scanId);
            }
        };
    }

    @Test
    public void testCancelOverrunScans()
            throws Exception {
        ScanWorkflow scanWorkflow = mock(ScanWorkflow.class);

        ScanOptions options = new ScanOptions("p0");
        ScanRangeStatus status = new ScanRangeStatus(0, "'0", ScanRange.all(), 0, Optional.<Integer>absent(), Optional.<Integer>absent());
        status.setScanQueuedTime(DateTime.now().minusMinutes(1).toDate());
        status.setScanStartTime(DateTime.now().minusMinutes(1).toDate());

        ScanStatusDAO scanStatusDAO = new InMemoryScanStatusDAO();
        scanStatusDAO.updateScanStatus(
                new ScanStatus("closedNew", options, false, DateTime.now().minusHours(2).toDate(),
                        ImmutableList.<ScanRangeStatus>of(), ImmutableList.<ScanRangeStatus>of(), ImmutableList.<ScanRangeStatus>of(status),
                        DateTime.now().minusHours(1).toDate()));
        scanStatusDAO.updateScanStatus(
                new ScanStatus("closedOld", options, false, DateTime.now().minusDays(2).toDate(),
                        ImmutableList.<ScanRangeStatus>of(), ImmutableList.<ScanRangeStatus>of(), ImmutableList.<ScanRangeStatus>of(),
                        DateTime.now().minusDays(1).minusHours(23).toDate()));
        scanStatusDAO.updateScanStatus(
                new ScanStatus("openNotOverrun", options, false, DateTime.now().minusDays(1).plusMinutes(1).toDate(),
                        ImmutableList.<ScanRangeStatus>of(), ImmutableList.<ScanRangeStatus>of(status), ImmutableList.<ScanRangeStatus>of(),
                        null));
        scanStatusDAO.updateScanStatus(
                new ScanStatus("openIsOverrun", options, false, DateTime.now().minusDays(1).minusMinutes(1).toDate(),
                        ImmutableList.<ScanRangeStatus>of(), ImmutableList.<ScanRangeStatus>of(status), ImmutableList.<ScanRangeStatus>of(),
                        null));

        ScheduledExecutorService service = mock(ScheduledExecutorService.class);

        LocalScanUploadMonitor monitor = new LocalScanUploadMonitor(scanWorkflow, scanStatusDAO, mock(ScanTableSetManager.class),
                mock(ScanWriterGenerator.class), mock(StashStateListener.class), mock(ScanCountListener.class),
                mock(DataTools.class), new InMemoryCompactionControlSource(), mock(DataCenters.class));
        monitor.setExecutorService(service);

        monitor.refreshScan("closedNew");
        monitor.refreshScan("closedOld");

        // Neither of the above should trigger an overrun check
        verify(service, never()).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        // This should schedule the check approximately one minute in the future
        monitor.refreshScan("openNotOverrun");

        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Long> executionTimeCaptor = ArgumentCaptor.forClass(Long.class);

        verify(service).schedule(runnableCaptor.capture(), executionTimeCaptor.capture(), eq(TimeUnit.MILLISECONDS));

        // Scheduled for one minute in the future with a few seconds slack for timing
        assertTrue(Range.closed(50000L, 60000L).contains(executionTimeCaptor.getValue()));
        // Verify running the Runnable cancels the scan
        runnableCaptor.getValue().run();
        assertTrue(scanStatusDAO.getScanStatus("openNotOverrun").isCanceled());
        verify(scanWorkflow).scanStatusUpdated("openNotOverrun");
        verifyNoMoreInteractions(service);

        // Repeat for the overrun scan
        reset(service);
        runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        executionTimeCaptor = ArgumentCaptor.forClass(Long.class);

        monitor.refreshScan("openIsOverrun");

        verify(service).schedule(runnableCaptor.capture(), executionTimeCaptor.capture(), eq(TimeUnit.MILLISECONDS));

        assertEquals(executionTimeCaptor.getValue().longValue(), 0);
        runnableCaptor.getValue().run();
        assertTrue(scanStatusDAO.getScanStatus("openIsOverrun").isCanceled());
        verify(scanWorkflow).scanStatusUpdated("openIsOverrun");

        verifyNoMoreInteractions(service, scanWorkflow);
    }

    @Test
    public void testCleanupOrphanedScans() {
        ScanWorkflow scanWorkflow = mock(ScanWorkflow.class);

        ScanTableSetManager tableSetManager = mock(ScanTableSetManager.class);
        when(tableSetManager.getAvailableTableSets()).thenReturn(ImmutableList.of("active", "closed", "unknown"));

        ScanOptions options = new ScanOptions("p0");
        ScanRangeStatus status = new ScanRangeStatus(0, "'0", ScanRange.all(), 0, Optional.<Integer>absent(), Optional.<Integer>absent());

        ScanStatusDAO scanStatusDAO = mock(ScanStatusDAO.class);
        when(scanStatusDAO.getScanStatus("active")).thenReturn(
                new ScanStatus("active", options, false, new Date(), ImmutableList.<ScanRangeStatus>of(status),
                        ImmutableList.<ScanRangeStatus>of(), ImmutableList.<ScanRangeStatus>of(), null));
        when(scanStatusDAO.getScanStatus("closed")).thenReturn(
                new ScanStatus("closed", options, false, new Date(), ImmutableList.<ScanRangeStatus>of(),
                        ImmutableList.<ScanRangeStatus>of(), ImmutableList.<ScanRangeStatus>of(status), new Date()));

        ScheduledExecutorService service = mock(ScheduledExecutorService.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation)
                    throws Throwable {
                ((Runnable) invocation.getArguments()[0]).run();
                return null;
            }
        }).when(service).submit(any((Runnable.class)));

        LocalScanUploadMonitor monitor = new LocalScanUploadMonitor(scanWorkflow, scanStatusDAO, tableSetManager,
                mock(ScanWriterGenerator.class), mock(StashStateListener.class), mock(ScanCountListener.class),
                mock(DataTools.class), new InMemoryCompactionControlSource(), mock(DataCenters.class));
        monitor.setExecutorService(service);

        monitor.cleanupOrphanedScans();

        verify(tableSetManager).getAvailableTableSets();
        verify(tableSetManager).cleanupTableSetForScan("closed");
        verify(tableSetManager).cleanupTableSetForScan("unknown");
        verifyNoMoreInteractions(tableSetManager);
    }

    @Test
    public void testScanTaskWithOversizedRange()
            throws Exception {
        final int shardId = 1;
        final long tableUuid = 100;
        final MetricRegistry metricRegistry = new MetricRegistry();

        DataTools dataTools = mock(DataTools.class);
        when(dataTools.multiTableScan(any(MultiTableScanOptions.class), any(TableSet.class), any(LimitCounter.class), any(ReadConsistency.class), any(DateTime.class)))
                .thenAnswer(new Answer<Iterator<MultiTableScanResult>>() {
                    @Override
                    public Iterator<MultiTableScanResult> answer(InvocationOnMock invocation)
                            throws Throwable {
                        // For this test return 400 results; if the test is successful it should only pull the first
                        // 300 anyway, but put a hard stop on it so the test cannot infinite loop on failure.
                        return new AbstractIterator<MultiTableScanResult>() {
                            int nextId = 0;

                            @Override
                            protected MultiTableScanResult computeNext() {
                                if (nextId == 400) {
                                    return endOfData();
                                }

                                String id = key(shardId, nextId++);
                                ByteBuffer rowKey = AstyanaxStorage.getRowKeyRaw(shardId, tableUuid, id);
                                Record record = mock(Record.class);
                                Key key = mock(Key.class);
                                when(key.getKey()).thenReturn(id);
                                Table table = mock(Table.class);
                                when(table.getName()).thenReturn("test:table");
                                when(key.getTable()).thenReturn(table);
                                when(record.getKey()).thenReturn(key);

                                return new MultiTableScanResult(rowKey, shardId, tableUuid, false, record);
                            }
                        };
                    }
                });

        when(dataTools.toContent(any(MultiTableScanResult.class), any(ReadConsistency.class), eq(false)))
                .thenAnswer(new Answer<Map<String, Object>>() {
                    @Override
                    public Map<String, Object> answer(InvocationOnMock invocation)
                            throws Throwable {
                        MultiTableScanResult result = (MultiTableScanResult) invocation.getArguments()[0];
                        return ImmutableMap.<String, Object>builder()
                                .put(Intrinsic.ID, result.getRecord().getKey().getKey())
                                .put(Intrinsic.TABLE, tableName(result.getTableUuid()))
                                .put(Intrinsic.DELETED, Boolean.FALSE)
                                .put(Intrinsic.VERSION, 1)
                                .build();
                    }
                });

        ScanWriter scanWriter = new DiscardingScanWriter(123, Optional.<Integer>absent(), metricRegistry);
        ScanWriterGenerator scanWriterGenerator = mock(ScanWriterGenerator.class);
        when(scanWriterGenerator.createScanWriter(eq(123), anySetOf(ScanDestination.class)))
                .thenReturn(scanWriter);

        TableSet tableSet = mock(TableSet.class);

        LocalRangeScanUploader uploader = new LocalRangeScanUploader(
                dataTools, scanWriterGenerator, new InMemoryCompactionControlSource(), mock(LifeCycleRegistry.class), metricRegistry, 2, 1000, Duration.standardMinutes(1),
                Duration.standardMinutes(5));

        ScanOptions options = new ScanOptions("p0")
                .setRangeScanSplitSize(100);

        RangeScanUploaderResult result;
        uploader.start();
        try {
            result = uploader.scanAndUpload(123, options, "p0", ScanRange.all(), tableSet, new Date());
        } finally {
            uploader.stop();
        }

        assertEquals(result.getStatus(), RangeScanUploaderResult.Status.REPSPLIT);

        ScanRange expectedResplitRange = ScanRange.create(
                AstyanaxStorage.getRowKeyRaw(shardId, tableUuid, key(shardId, 299)),
                ScanRange.MAX_VALUE);
        assertEquals(expectedResplitRange, result.getResplitRange());
    }

    @Test
    public void testScanRangeTaskCompletesWithResplitOnWithOversizedRange()
            throws Exception {
        String id = "test";
        String placement = "p0";
        ScanRange originalRange = ScanRange.create(asByteBuffer(1, 2), asByteBuffer(10, 100));
        ScanRange resplitRange = ScanRange.create(asByteBuffer(5, 50), asByteBuffer(10, 100));
        ScanRange adjustedRange = ScanRange.create(asByteBuffer(1, 2), asByteBuffer(5, 50));

        ScanWorkflow scanWorkflow = mock(ScanWorkflow.class);
        RangeScanUploader rangeScanUploader = mock(RangeScanUploader.class);
        ScanTableSetManager scanTableSetManager = mock(ScanTableSetManager.class);

        when(scanTableSetManager.getTableSetForScan(id)).thenReturn(mock(TableSet.class));

        ScanRangeTask task = mock(ScanRangeTask.class);
        when(task.getScanId()).thenReturn(id);
        when(task.getId()).thenReturn(123);
        when(task.getPlacement()).thenReturn(placement);
        when(task.getRange()).thenReturn(originalRange);

        when(scanWorkflow.claimScanRangeTasks(anyInt(), any(Duration.class)))
                .thenReturn(ImmutableList.of(task), ImmutableList.<ScanRangeTask>of());

        ScanOptions options = new ScanOptions(placement);

        List<ScanRangeStatus> statuses = ImmutableList.of(
                new ScanRangeStatus(123, placement, originalRange, 15, Optional.<Integer>absent(), Optional.<Integer>absent()));

        ScanStatus scanStatus = new ScanStatus(id, options, false, new Date(), statuses,
                Lists.<ScanRangeStatus>newArrayList(), Lists.<ScanRangeStatus>newArrayList());

        ScanStatusDAO scanStatusDAO = new InMemoryScanStatusDAO();
        scanStatusDAO.updateScanStatus(scanStatus);

        when(rangeScanUploader.scanAndUpload(eq(123), eq(options), eq(placement), eq(originalRange), any(TableSet.class), any(Date.class)))
                .thenReturn(RangeScanUploaderResult.resplit(resplitRange));

        DistributedScanRangeMonitor distributedScanRangeMonitor = new DistributedScanRangeMonitor(
                scanWorkflow, scanStatusDAO, rangeScanUploader, scanTableSetManager, 1, mock(LifeCycleRegistry.class));

        distributedScanRangeMonitor.setExecutorServices(MoreExecutors.sameThreadExecutor(), mock(ScheduledExecutorService.class));
        distributedScanRangeMonitor.startScansIfAvailable();

        scanStatus = scanStatusDAO.getScanStatus(id);
        assertEquals(scanStatus.getCompleteScanRanges().size(), 1);

        ScanRangeStatus rangeStatus = scanStatus.getCompleteScanRanges().get(0);
        assertNotNull(rangeStatus.getScanCompleteTime());
        assertEquals(rangeStatus.getScanRange(), adjustedRange);
        assertEquals(rangeStatus.getResplitRange(), Optional.of(resplitRange));
    }

    @Test
    public void testResplitStoredAndRescheduled()
            throws Exception {
        String id = "test";
        String placement = "p0";
        ScanRange completeRange = ScanRange.create(asByteBuffer(1, 2), asByteBuffer(5, 50));
        ScanRange resplitRange = ScanRange.create(asByteBuffer(5, 50), asByteBuffer(10, 100));

        ScanOptions options = new ScanOptions(placement);

        ScanRangeStatus status = new ScanRangeStatus(0, placement, completeRange, 15, Optional.<Integer>absent(), Optional.<Integer>absent());
        status.setScanQueuedTime(new Date());
        status.setScanStartTime(new Date());
        status.setScanCompleteTime(new Date());
        status.setResplitRange(resplitRange);

        List<ScanRangeStatus> statuses = ImmutableList.of(status);

        ScanStatus scanStatus = new ScanStatus(id, options, false, new Date(),
                Lists.<ScanRangeStatus>newArrayList(), Lists.<ScanRangeStatus>newArrayList(), statuses);

        ScanStatusDAO scanStatusDAO = new InMemoryScanStatusDAO();
        scanStatusDAO.updateScanStatus(scanStatus);

        ScanWorkflow scanWorkflow = mock(ScanWorkflow.class);

        DataTools dataTools = mock(DataTools.class);
        when(dataTools.getScanRangeSplits(placement, options.getRangeScanSplitSize(), Optional.of(resplitRange)))
                .thenReturn(ScanRangeSplits.builder()
                        .addScanRange("1a", "hostIP", ScanRange.create(asByteBuffer(5, 50), asByteBuffer(6, 60)))
                        .addScanRange("1a", "hostIP", ScanRange.create(asByteBuffer(6, 60), asByteBuffer(7, 70)))
                        .addScanRange("1a", "hostIP", ScanRange.create(asByteBuffer(7, 70), asByteBuffer(10, 100)))
                        .build());

        LocalScanUploadMonitor monitor = new LocalScanUploadMonitor(scanWorkflow, scanStatusDAO,
                mock(ScanTableSetManager.class), mock(ScanWriterGenerator.class), mock(StashStateListener.class),
                mock(ScanCountListener.class), dataTools, new InMemoryCompactionControlSource(), mock(DataCenters.class));
        monitor.setExecutorService(mock(ScheduledExecutorService.class));

        monitor.refreshScan(id);

        scanStatus = scanStatusDAO.getScanStatus(id);
        assertTrue(scanStatus.getActiveScanRanges().isEmpty());
        assertEquals(scanStatus.getCompleteScanRanges().size(), 1);

        ScanRangeStatus rangeStatus = scanStatus.getCompleteScanRanges().get(0);
        assertEquals(rangeStatus.getTaskId(), 0);
        assertEquals(rangeStatus.getScanRange(), completeRange);
        assertEquals(rangeStatus.getResplitRange(), Optional.<ScanRange>absent());

        Set<ScanRangeStatus> expectedPendingStatuses = ImmutableSet.of(
                new ScanRangeStatus(1, placement, ScanRange.create(asByteBuffer(5, 50), asByteBuffer(6, 60)), 15, Optional.<Integer>absent(), Optional.<Integer>absent()),
                new ScanRangeStatus(2, placement, ScanRange.create(asByteBuffer(6, 60), asByteBuffer(7, 70)), 15, Optional.<Integer>absent(), Optional.<Integer>absent()),
                new ScanRangeStatus(3, placement, ScanRange.create(asByteBuffer(7, 70), asByteBuffer(10, 100)), 15, Optional.<Integer>absent(), Optional.<Integer>absent()));

        // Ensure queued time for the pending scan ranges was set, then remove it to make the subsequent equality assertion valid.
        for (ScanRangeStatus pendingScanRange : scanStatus.getPendingScanRanges()) {
            assertNotNull(pendingScanRange.getScanQueuedTime());
            pendingScanRange.setScanQueuedTime(null);
        }

        assertEquals(ImmutableSet.copyOf(scanStatus.getPendingScanRanges()), expectedPendingStatuses);
    }

    @Test
    public void testS3UploadFailure()
            throws Exception {
        MetricRegistry metricRegistry = new MetricRegistry();
        final ScheduledExecutorService uploadService = Executors.newScheduledThreadPool(1);
        LocalRangeScanUploader scanUploader = null;

        try {
            AmazonS3 amazonS3 = mock(AmazonS3.class);
            when(amazonS3.putObject(any(PutObjectRequest.class)))
                    .thenThrow(new AmazonClientException("Simulated putObject exception"));

            S3ScanWriter s3ScanWriter = new S3ScanWriter(
                    1, URI.create("http://dummy-s3-bucket/root"), Optional.of(10), metricRegistry, amazonS3, uploadService);
            s3ScanWriter.setRetryDelay(Duration.millis(1));

            ScanWriterGenerator scanWriterGenerator = mock(ScanWriterGenerator.class);
            when(scanWriterGenerator.createScanWriter(eq(1), anySetOf(ScanDestination.class)))
                    .thenReturn(s3ScanWriter);

            DataTools dataTools = mock(DataTools.class);

            Table table = mock(Table.class);
            when(table.getName()).thenReturn("test:table");

            Key key = mock(Key.class);
            when(key.getTable()).thenReturn(table);
            when(key.getKey()).thenReturn("foo");

            Record record = mock(Record.class);
            when(record.getKey()).thenReturn(key);

            when(dataTools.multiTableScan(any(MultiTableScanOptions.class), any(TableSet.class), any(LimitCounter.class), any(ReadConsistency.class), any(DateTime.class)))
                    .thenReturn(Iterators.singletonIterator(
                            new MultiTableScanResult(
                                    ByteBuffer.wrap(new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
                                    1, 100, false, record)));

            when(dataTools.toContent(any(MultiTableScanResult.class), any(ReadConsistency.class), eq(false)))
                    .thenAnswer(new Answer<Map<String, Object>>() {
                        @Override
                        public Map<String, Object> answer(InvocationOnMock invocation)
                                throws Throwable {
                            MultiTableScanResult result = (MultiTableScanResult) invocation.getArguments()[0];
                            return ImmutableMap.<String, Object>builder()
                                    .put(Intrinsic.ID, result.getRecord().getKey().getKey())
                                    .put(Intrinsic.TABLE, tableName(result.getTableUuid()))
                                    .put(Intrinsic.DELETED, Boolean.FALSE)
                                    .put(Intrinsic.VERSION, 1)
                                    .build();
                        }
                    });

            TableSet tableSet = mock(TableSet.class);

            scanUploader = new LocalRangeScanUploader(dataTools, scanWriterGenerator, new InMemoryCompactionControlSource(), mock(LifeCycleRegistry.class), metricRegistry);
            scanUploader.start();

            ScanOptions scanOptions = new ScanOptions("p0")
                    .addDestination(ScanDestination.discard());

            RangeScanUploaderResult result = scanUploader.scanAndUpload(1, scanOptions, "p0", ScanRange.all(), tableSet, new Date());
            assertEquals(result.getStatus(), RangeScanUploaderResult.Status.FAILURE);
        } finally {
            uploadService.shutdownNow();
            if (scanUploader != null) {
                scanUploader.stop();
            }
        }
    }

    @Test
    public void testFailsScanRangeTaskWithHungTransfer()
            throws Exception {
        MetricRegistry metricRegistry = new MetricRegistry();
        DataTools dataTools = mock(DataTools.class);

        Table table = mock(Table.class);
        when(table.getName()).thenReturn("test:table");

        Key key = mock(Key.class);
        when(key.getTable()).thenReturn(table);
        when(key.getKey()).thenReturn("foo");

        Record record = mock(Record.class);
        when(record.getKey()).thenReturn(key);

        when(dataTools.multiTableScan(any(MultiTableScanOptions.class), any(TableSet.class), any(LimitCounter.class), any(ReadConsistency.class), any(DateTime.class)))
                .thenReturn(Iterators.singletonIterator(
                        new MultiTableScanResult(
                                ByteBuffer.wrap(new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
                                1, 100, false, record)));

        when(dataTools.toContent(any(MultiTableScanResult.class), any(ReadConsistency.class), eq(false)))
                .thenAnswer(new Answer<Map<String, Object>>() {
                    @Override
                    public Map<String, Object> answer(InvocationOnMock invocation)
                            throws Throwable {
                        MultiTableScanResult result = (MultiTableScanResult) invocation.getArguments()[0];
                        return ImmutableMap.<String, Object>builder()
                                .put(Intrinsic.ID, result.getRecord().getKey().getKey())
                                .put(Intrinsic.TABLE, tableName(result.getTableUuid()))
                                .put(Intrinsic.DELETED, Boolean.FALSE)
                                .put(Intrinsic.VERSION, 1)
                                .build();
                    }
                });

        S3ScanWriter scanWriter = mock(S3ScanWriter.class);
        when(scanWriter.writeShardRows(anyString(), anyString(), anyInt(), anyLong()))
                .thenReturn(new DiscardingScanWriter(0, Optional.<Integer>absent(), metricRegistry).writeShardRows("test:table", "p0", 0, 0));
        when(scanWriter.waitForAllTransfersComplete(any(Duration.class)))
                .thenAnswer(new Answer<WaitForAllTransfersCompleteResult>() {
                    @Override
                    public WaitForAllTransfersCompleteResult answer(InvocationOnMock invocation)
                            throws Throwable {
                        Duration duration = (Duration) invocation.getArguments()[0];
                        Thread.sleep(duration.getMillis());
                        TransferKey transferKey = new TransferKey(0, 0);
                        return new WaitForAllTransfersCompleteResult(
                                ImmutableMap.of(transferKey, new TransferStatus(transferKey, 100, 1, 0)));
                    }
                });

        ScanWriterFactory scanWriterFactory = mock(ScanWriterFactory.class);
        when(scanWriterFactory.createS3ScanWriter(anyInt(), any(URI.class), any(Optional.class)))
                .thenReturn(scanWriter);

        ScanWriterGenerator scanWriterGenerator = new ScanWriterGenerator(scanWriterFactory);

        ScanOptions options = new ScanOptions(ImmutableList.of("p0"))
                .addDestination(ScanDestination.to(URI.create("s3://bucket/test")));

        LocalRangeScanUploader uploader = new LocalRangeScanUploader(
                dataTools, scanWriterGenerator, new InMemoryCompactionControlSource(), mock(LifeCycleRegistry.class), metricRegistry, 2, 1000, Duration.millis(100),
                Duration.standardSeconds(1));
        uploader.start();

        try {
            RangeScanUploaderResult result = uploader.scanAndUpload(0, options, "p0", ScanRange.all(), mock(TableSet.class), mock(Date.class));
            assertEquals(result.getStatus(), RangeScanUploaderResult.Status.FAILURE);
        } finally {
            uploader.stop();
        }
    }

    @Test
    public void testPassesScanRangeTaskWithSlowTransfer()
            throws Exception {
        DataTools dataTools = mock(DataTools.class);
        MetricRegistry metricRegistry = new MetricRegistry();

        Table table = mock(Table.class);
        when(table.getName()).thenReturn("test:table");

        Key key = mock(Key.class);
        when(key.getTable()).thenReturn(table);
        when(key.getKey()).thenReturn("foo");

        Record record = mock(Record.class);
        when(record.getKey()).thenReturn(key);

        when(dataTools.multiTableScan(any(MultiTableScanOptions.class), any(TableSet.class), any(LimitCounter.class), any(ReadConsistency.class), any(DateTime.class)))
                .thenReturn(Iterators.singletonIterator(
                        new MultiTableScanResult(
                                ByteBuffer.wrap(new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
                                1, 100, false, record)));

        when(dataTools.toContent(any(MultiTableScanResult.class), any(ReadConsistency.class), eq(false)))
                .thenAnswer(new Answer<Map<String, Object>>() {
                    @Override
                    public Map<String, Object> answer(InvocationOnMock invocation)
                            throws Throwable {
                        MultiTableScanResult result = (MultiTableScanResult) invocation.getArguments()[0];
                        return ImmutableMap.<String, Object>builder()
                                .put(Intrinsic.ID, result.getRecord().getKey().getKey())
                                .put(Intrinsic.TABLE, tableName(result.getTableUuid()))
                                .put(Intrinsic.DELETED, Boolean.FALSE)
                                .put(Intrinsic.VERSION, 1)
                                .build();
                    }
                });

        S3ScanWriter scanWriter = mock(S3ScanWriter.class);
        when(scanWriter.writeShardRows(anyString(), anyString(), anyInt(), anyLong()))
                .thenReturn(new DiscardingScanWriter(0, Optional.<Integer>absent(), metricRegistry).writeShardRows("test:table", "p0", 0, 0));
        when(scanWriter.waitForAllTransfersComplete(any(Duration.class)))
                .thenAnswer(new Answer<WaitForAllTransfersCompleteResult>() {
                    int _call = -1;

                    @Override
                    public WaitForAllTransfersCompleteResult answer(InvocationOnMock invocation)
                            throws Throwable {
                        // Simulated uploading one byte at a time for 20 calls in 2 attempts at 10 bytes per attempt
                        // before succeeding
                        if (++_call == 20) {
                            return new WaitForAllTransfersCompleteResult(ImmutableMap.<TransferKey, TransferStatus>of());
                        }

                        Duration duration = (Duration) invocation.getArguments()[0];
                        Thread.sleep(duration.getMillis());
                        TransferKey transferKey = new TransferKey(0, 0);
                        int attempt = _call / 10 + 1;
                        long bytesTransferred = _call % 10;
                        return new WaitForAllTransfersCompleteResult(
                                ImmutableMap.of(transferKey, new TransferStatus(transferKey, 20, attempt, bytesTransferred)));
                    }
                });

        ScanWriterFactory scanWriterFactory = mock(ScanWriterFactory.class);
        when(scanWriterFactory.createS3ScanWriter(anyInt(), any(URI.class), any(Optional.class)))
                .thenReturn(scanWriter);

        ScanWriterGenerator scanWriterGenerator = new ScanWriterGenerator(scanWriterFactory);

        ScanOptions options = new ScanOptions(ImmutableList.of("p0"))
                .addDestination(ScanDestination.to(URI.create("s3://bucket/test")));

        LocalRangeScanUploader uploader = new LocalRangeScanUploader(
                dataTools, scanWriterGenerator, new InMemoryCompactionControlSource(), mock(LifeCycleRegistry.class), metricRegistry, 2, 1000, Duration.millis(100),
                Duration.millis(100));
        uploader.start();

        try {
            // Given our configuration above the file will take 2 seconds to upload but we are configured to terminate
            // the upload if there was no progress for 100ms.  Therefore if this returns success then the upload
            // was not killed despite taking over 100ms, which is what this test is checking for.
            RangeScanUploaderResult result = uploader.scanAndUpload(0, options, "p0", ScanRange.all(), mock(TableSet.class), mock(Date.class));
            assertEquals(result.getStatus(), RangeScanUploaderResult.Status.SUCCESS);
        } finally {
            uploader.stop();
        }
    }

    private DataCenter mockDataCenter(String name, String adminUri, String serviceUri) {
        DataCenter dc = mock(DataCenter.class);
        when(dc.getName()).thenReturn(name);
        when(dc.getAdminUri()).thenReturn(URI.create(adminUri));
        when(dc.getServiceUri()).thenReturn(URI.create(serviceUri));
        return dc;
    }
}
