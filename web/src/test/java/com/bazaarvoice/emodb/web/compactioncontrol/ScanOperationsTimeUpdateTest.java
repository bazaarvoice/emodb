package com.bazaarvoice.emodb.web.compactioncontrol;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.compactioncontrol.InMemoryCompactionControlSource;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.MultiTableScanOptions;
import com.bazaarvoice.emodb.sor.db.MultiTableScanResult;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.db.ScanRangeSplits;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.ScanUploader;
import com.bazaarvoice.emodb.web.scanner.control.InMemoryScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.control.ScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.scanstatus.InMemoryScanStatusDAO;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatusDAO;
import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import static java.lang.String.format;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScanOperationsTimeUpdateTest {

    @Test
    public void testTimeIsUpdatedWhenScanStartsAndItsDeletedAfterScanIsFinished()
            throws Exception {
        StashStateListener stashStateListener = mock(StashStateListener.class);
        DataCenters dataCenters = mock(DataCenters.class);
        DataCenter dataCenter1 = mockDataCenter("us-east", "http://emodb.cert.us-east-1.nexus.bazaarvoice.com:8081", "http://emodb.cert.us-east-1.nexus.bazaarvoice.com:8080");
        when(dataCenters.getSelf()).thenReturn(dataCenter1);
        when(dataCenters.getAll()).thenReturn(ImmutableList.of(dataCenter1));
        ScanWorkflow scanWorkflow = new InMemoryScanWorkflow();
        ScanStatusDAO scanStatusDAO = new InMemoryScanStatusDAO();
        ScanOptions scanOptions = new ScanOptions("placement1").addDestination(ScanDestination.to(new URI("s3://testbucket/test/path")));

        CompactionControlSource compactionControlSource = new InMemoryCompactionControlSource();

        // start the scan
        ScanUploader scanUploader = new ScanUploader(getDataTools(), scanWorkflow, scanStatusDAO, stashStateListener, compactionControlSource, dataCenters);
        // the default in code is 1 minute, but we don't want to wait that long in the test, so set to a smaller value.
        scanUploader.setCompactionControlBufferTimeInMillis(Duration.standardSeconds(1).getMillis());
        scanUploader.scanAndUpload("test1", scanOptions).get();
        Assert.assertEquals(compactionControlSource.getAllStashTimes().size(), 1);
        Assert.assertEquals(compactionControlSource.getAllStashTimes().containsKey("test1"), true);

        // cancel the scan
        scanUploader.cancel("test1");
        Assert.assertEquals(compactionControlSource.getAllStashTimes().size(), 0);
        Assert.assertEquals(compactionControlSource.getAllStashTimes().containsKey("test1"), false);
    }

    @Test
    public void testTimeEntryDoNotExistIfScanFailsWithAnException()
            throws Exception {
        StashStateListener stashStateListener = mock(StashStateListener.class);
        DataCenters dataCenters = mock(DataCenters.class);
        DataCenter dataCenter1 = mockDataCenter("us-east", "http://emodb.cert.us-east-1.nexus.bazaarvoice.com:8081", "http://emodb.cert.us-east-1.nexus.bazaarvoice.com:8080");
        when(dataCenters.getSelf()).thenReturn(dataCenter1);
        when(dataCenters.getAll()).thenReturn(ImmutableList.of(dataCenter1));
        ScanWorkflow scanWorkflow = new InMemoryScanWorkflow();
        ScanOptions scanOptions = new ScanOptions("placement1").addDestination(ScanDestination.to(new URI("s3://testbucket/test/path")));

        ScanStatusDAO scanStatusDAO = mock(ScanStatusDAO.class);
        doThrow(Exception.class).when(scanStatusDAO).updateScanStatus(any(ScanStatus.class));

        CompactionControlSource compactionControlSource = new InMemoryCompactionControlSource();

        // start the scan which will throw an exception
        ScanUploader scanUploader = new ScanUploader(getDataTools(), scanWorkflow, scanStatusDAO, stashStateListener, compactionControlSource, dataCenters);
        // the default in code is 1 minute, but we don't want to wait that long in the test, so set to a smaller value.
        scanUploader.setCompactionControlBufferTimeInMillis(Duration.standardSeconds(1).getMillis());
        try {
            scanUploader.scanAndUpload("test1", scanOptions).get();
        } catch (Exception e) {
            // expected as the exception is propagated.
        }

        Assert.assertEquals(compactionControlSource.getAllStashTimes().size(), 0);
        Assert.assertEquals(compactionControlSource.getAllStashTimes().containsKey("test1"), false);
    }

    /***
     * helper methods
     ***/
    private DataCenter mockDataCenter(String name, String adminUri, String serviceUri) {
        DataCenter dc = mock(DataCenter.class);
        when(dc.getName()).thenReturn(name);
        when(dc.getAdminUri()).thenReturn(URI.create(adminUri));
        when(dc.getServiceUri()).thenReturn(URI.create(serviceUri));
        return dc;
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
                when(table.getName()).thenReturn(format("table%02d", tableUuid));
                when(key.getTable()).thenReturn(table);
                when(record.getKey()).thenReturn(key);
                when(table.getOptions()).thenReturn(new TableOptionsBuilder().setPlacement("placement1").build());
                return new MultiTableScanResult(AstyanaxStorage.getRowKeyRaw(shard, tableUuid, keyString), shard, tableUuid, false, record);
            }
        };
    }

    private String key(int shard, int row) {
        return format("%02d%02d", shard, row);
    }

    private DataTools getDataTools() {
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
                                .put(Intrinsic.TABLE, format("table%02d", result.getTableUuid()))
                                .put(Intrinsic.DELETED, Boolean.FALSE)
                                .put(Intrinsic.VERSION, 1)
                                .build();
                    }
                });

        return dataTools;
    }
}