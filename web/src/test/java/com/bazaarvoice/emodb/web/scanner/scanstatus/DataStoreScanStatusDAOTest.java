package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.kafka.KafkaProducerService;
import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class DataStoreScanStatusDAOTest {

    private DataStore _dataStore;
    private DataStoreScanStatusDAO _dao;

    @BeforeMethod
    public void setUp() {
        _dataStore = new InMemoryDataStore(new MetricRegistry(), mock(KafkaProducerService.class));
        _dao = new DataStoreScanStatusDAO(_dataStore, "scan_table", "app_global:sys");
    }

    @Test
    public void testUpdate() {
        ScanOptions options = new ScanOptions(ImmutableList.of("p0"));
        options.addDestination(ScanDestination.to(URI.create("s3://bucket/path/to/root")));

        ScanRangeStatus pending = new ScanRangeStatus(0, "p0",
                        ScanRange.create(ByteBuffer.wrap(new byte[] {0x00}), ByteBuffer.wrap(new byte[] {0x01})),
                        0, Optional.of(1), Optional.of(3));

        ScanRangeStatus active = new ScanRangeStatus(1, "p0",
                ScanRange.create(ByteBuffer.wrap(new byte[]{0x02}), ByteBuffer.wrap(new byte[]{0x03})),
                1, Optional.empty(), Optional.empty());
        active.setScanQueuedTime(Date.from(Instant.now().minus(Duration.ofMinutes(10))));
        active.setScanStartTime(Date.from(Instant.now().minus(Duration.ofMinutes(5))));

        ScanRangeStatus complete = new ScanRangeStatus(2, "p0",
                ScanRange.create(ByteBuffer.wrap(new byte[]{0x04}), ByteBuffer.wrap(new byte[]{0x05})),
                2, Optional.empty(), Optional.empty());
        complete.setScanQueuedTime(Date.from(Instant.now().minus(Duration.ofMinutes(30))));
        complete.setScanStartTime(Date.from(Instant.now().minus(Duration.ofMinutes(25))));
        complete.setScanCompleteTime(Date.from(Instant.now().minus(Duration.ofMinutes(20))));

        ScanStatus scanStatus = new ScanStatus("id", options, false, false, new Date(),
                ImmutableList.of(pending), ImmutableList.of(active), ImmutableList.of(complete));

        _dao.updateScanStatus(scanStatus);

        ScanStatus returned = _dao.getScanStatus("id");
        assertEquals(returned, scanStatus);
    }

    @Test
    public void testSetScanRangeQueued() {
        ScanOptions options = new ScanOptions(ImmutableList.of("p0"));
        options.addDestination(ScanDestination.to(URI.create("s3://bucket/path/to/root")));

        ScanRangeStatus pending = new ScanRangeStatus(0, "p0", ScanRange.all(), 0,
                Optional.empty(), Optional.empty());

        ScanStatus scanStatus = new ScanStatus("id", options, true, false, new Date(),
                ImmutableList.of(pending), ImmutableList.of(), ImmutableList.of());

        _dao.updateScanStatus(scanStatus);

        Date queuedTime = Date.from(Instant.now().minus(Duration.ofSeconds(1)));
        _dao.setScanRangeTaskQueued("id", 0, queuedTime);

        ScanStatus returned = _dao.getScanStatus("id");
        assertEquals(returned.getPendingScanRanges().size(), 1);
        assertTrue(returned.getActiveScanRanges().isEmpty());
        assertTrue(returned.getCompleteScanRanges().isEmpty());
        assertEquals(returned.getPendingScanRanges().get(0).getScanQueuedTime(), queuedTime);

        // Setting queued again should not change the queue time
        _dao.setScanRangeTaskQueued("id", 0, new Date());

        returned = _dao.getScanStatus("id");
        assertEquals(returned.getPendingScanRanges().get(0).getScanQueuedTime(), queuedTime);
    }

    @Test
    public void testSetScanRangeActive()
            throws Exception {
        ScanOptions options = new ScanOptions(ImmutableList.of("p0"));
        options.addDestination(ScanDestination.to(URI.create("s3://bucket/path/to/root")));

        ScanRangeStatus pending = new ScanRangeStatus(0, "p0", ScanRange.all(), 0,
                Optional.empty(), Optional.empty());
        pending.setScanQueuedTime(Date.from(Instant.now().minus(Duration.ofMinutes(1))));

        ScanStatus scanStatus = new ScanStatus("id", options, true, false, new Date(),
                ImmutableList.of(pending), ImmutableList.of(), ImmutableList.of());
        _dao.updateScanStatus(scanStatus);

        Date startTime = Date.from(Instant.now().minus(Duration.ofSeconds(1)));
        _dao.setScanRangeTaskActive("id", 0, startTime);

        ScanStatus returned = _dao.getScanStatus("id");
        assertEquals(returned.getActiveScanRanges().size(), 1);
        assertTrue(returned.getPendingScanRanges().isEmpty());
        assertTrue(returned.getCompleteScanRanges().isEmpty());
        assertEquals(returned.getActiveScanRanges().get(0).getScanStartTime(), startTime);

        // Setting active again should not change the start time
        _dao.setScanRangeTaskActive("id", 0, new Date());

        returned = _dao.getScanStatus("id");
        assertEquals(returned.getActiveScanRanges().get(0).getScanStartTime(), startTime);
    }

    @Test
    public void testSetScanRangeComplete() {
        ScanOptions options = new ScanOptions(ImmutableList.of("p0"));
        options.addDestination(ScanDestination.to(URI.create("s3://bucket/path/to/root")));

        ScanRangeStatus active = new ScanRangeStatus(0, "p0", ScanRange.all(), 0,
                Optional.empty(), Optional.empty());
        active.setScanQueuedTime(Date.from(Instant.now().minus(Duration.ofMinutes(2))));
        active.setScanStartTime(Date.from(Instant.now().minus(Duration.ofMinutes(1))));

        ScanStatus scanStatus = new ScanStatus("id", options, true, false, new Date(),
                ImmutableList.of(), ImmutableList.of(active), ImmutableList.of());
        _dao.updateScanStatus(scanStatus);

        Date completeTime = Date.from(Instant.now().minusSeconds(1));
        _dao.setScanRangeTaskComplete("id", 0, completeTime);

        ScanStatus returned = _dao.getScanStatus("id");
        assertEquals(returned.getCompleteScanRanges().size(), 1);
        assertTrue(returned.getPendingScanRanges().isEmpty());
        assertTrue(returned.getActiveScanRanges().isEmpty());
        assertEquals(returned.getCompleteScanRanges().get(0).getScanCompleteTime(), completeTime);

        // Setting complete again should not change the complete time
        _dao.setScanRangeTaskComplete("id", 0, new Date());

        returned = _dao.getScanStatus("id");
        assertEquals(returned.getCompleteScanRanges().get(0).getScanCompleteTime(), completeTime);
    }

    @Test
    public void testSetScanRangeInactive() {
        ScanOptions options = new ScanOptions(ImmutableList.of("p0"));
        options.addDestination(ScanDestination.to(URI.create("s3://bucket/path/to/root")));

        ScanRangeStatus active = new ScanRangeStatus(0, "p0", ScanRange.all(), 0,
                Optional.empty(), Optional.empty());
        active.setScanQueuedTime(new Date());
        active.setScanStartTime(new Date());

        ScanStatus scanStatus = new ScanStatus("id", options, true, false, new Date(),
                ImmutableList.of(), ImmutableList.of(active), ImmutableList.of());
        _dao.updateScanStatus(scanStatus);

        _dao.setScanRangeTaskInactive("id", 0);

        ScanStatus returned = _dao.getScanStatus("id");
        assertEquals(returned.getPendingScanRanges().size(), 1);
        assertTrue(returned.getActiveScanRanges().isEmpty());
        assertTrue(returned.getCompleteScanRanges().isEmpty());

        assertNull(returned.getPendingScanRanges().get(0).getScanQueuedTime());
        assertNull(returned.getPendingScanRanges().get(0).getScanStartTime());
        assertNull(returned.getPendingScanRanges().get(0).getScanCompleteTime());
    }

    @Test
    public void testSetScanRangePartiallyComplete() {
        ScanOptions options = new ScanOptions(ImmutableList.of("p0"));
        options.addDestination(ScanDestination.to(URI.create("s3://bucket/path/to/root")));

        ScanRangeStatus active = new ScanRangeStatus(0, "p0", ScanRange.all(), 0,
                Optional.empty(), Optional.empty());
        active.setScanQueuedTime(new Date());
        active.setScanStartTime(new Date());

        ScanStatus scanStatus = new ScanStatus("id", options, true, false, new Date(),
                ImmutableList.of(), ImmutableList.of(active), ImmutableList.of());
        _dao.updateScanStatus(scanStatus);

        ScanRange completeRange = ScanRange.create(ScanRange.MIN_VALUE, ByteBuffer.wrap(new byte[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}));
        ScanRange resplitRange = ScanRange.create(completeRange.getFrom(), ScanRange.MAX_VALUE);
        Date completeTime = new Date();

        _dao.setScanRangeTaskPartiallyComplete("id", 0, completeRange, resplitRange, completeTime);

        ScanStatus returned = _dao.getScanStatus("id");
        assertEquals(returned.getCompleteScanRanges().size(), 1);
        assertTrue(returned.getPendingScanRanges().isEmpty());
        assertTrue(returned.getActiveScanRanges().isEmpty());

        assertEquals(returned.getCompleteScanRanges().get(0).getScanCompleteTime(), completeTime);
        assertEquals(returned.getCompleteScanRanges().get(0).getScanRange(), completeRange);
        assertEquals(returned.getCompleteScanRanges().get(0).getResplitRange(), Optional.of(resplitRange));
    }

    @Test
    public void testSetScanRangeResplit() {
        ScanOptions options = new ScanOptions(ImmutableList.of("p0"));
        options.addDestination(ScanDestination.to(URI.create("s3://bucket/path/to/root")));

        ByteBuffer row1 = ByteBuffer.wrap(new byte[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1});
        ByteBuffer row2 = ByteBuffer.wrap(new byte[] { 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2});
        ByteBuffer row3 = ByteBuffer.wrap(new byte[] { 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3});

        ScanRangeStatus complete = new ScanRangeStatus(0, "p0", ScanRange.create(ScanRange.MIN_VALUE, row1), 0,
                Optional.empty(), Optional.empty());
        complete.setScanQueuedTime(new Date());
        complete.setScanStartTime(new Date());
        complete.setScanCompleteTime(new Date());
        complete.setResplitRange(ScanRange.create(row1, ScanRange.MAX_VALUE));

        ScanStatus scanStatus = new ScanStatus("id", options, true, false, new Date(),
                ImmutableList.of(), ImmutableList.of(), ImmutableList.of(complete));
        _dao.updateScanStatus(scanStatus);

        List<ScanRangeStatus> resplits = ImmutableList.of(
                new ScanRangeStatus(1, "p0", ScanRange.create(row1, row2), 0, Optional.empty(), Optional.empty()),
                new ScanRangeStatus(2, "p0", ScanRange.create(row2, row3), 0, Optional.empty(), Optional.empty()),
                new ScanRangeStatus(3, "p0", ScanRange.create(row3, ScanRange.MAX_VALUE), 0, Optional.empty(), Optional.empty()));

        _dao.resplitScanRangeTask("id", 0, resplits);

        ScanStatus returned = _dao.getScanStatus("id");
        assertEquals(returned.getCompleteScanRanges().size(), 1);
        assertEquals(returned.getPendingScanRanges().size(), 3);
        assertTrue(returned.getActiveScanRanges().isEmpty());

        assertEquals(returned.getCompleteScanRanges().get(0).getResplitRange(), Optional.<ScanRange>empty());
        assertEquals(ImmutableSet.copyOf(returned.getPendingScanRanges()), ImmutableSet.copyOf(resplits));
    }

    @Test
    public void testSetCanceled() {
        ScanOptions options = new ScanOptions(ImmutableList.of("p0"));
        options.addDestination(ScanDestination.to(URI.create("s3://bucket/path/to/root")));

        ScanRangeStatus active = new ScanRangeStatus(0, "p0", ScanRange.all(), 0,
                Optional.empty(), Optional.empty());
        active.setScanQueuedTime(new Date());
        active.setScanStartTime(new Date());

        ScanStatus scanStatus = new ScanStatus("id", options, true, false, new Date(),
                ImmutableList.of(), ImmutableList.of(active), ImmutableList.of());
        _dao.updateScanStatus(scanStatus);

        _dao.setCanceled("id");

        ScanStatus returned = _dao.getScanStatus("id");
        assertTrue(returned.isCanceled());
    }

    @Test
    public void testSetCompleteTime() {
        ScanOptions options = new ScanOptions(ImmutableList.of("p0"));
        options.addDestination(ScanDestination.to(URI.create("s3://bucket/path/to/root")));

        ScanRangeStatus complete = new ScanRangeStatus(0, "p0", ScanRange.all(), 0,
                Optional.empty(), Optional.empty());
        complete.setScanQueuedTime(new Date());
        complete.setScanStartTime(new Date());
        complete.setScanCompleteTime(new Date());

        ScanStatus scanStatus = new ScanStatus("id", options, true, false, new Date(),
                ImmutableList.of(), ImmutableList.of(), ImmutableList.of(complete));
        _dao.updateScanStatus(scanStatus);

        Date completeTime = new Date();
        _dao.setCompleteTime("id", completeTime);

        ScanStatus returned = _dao.getScanStatus("id");
        assertEquals(returned.getCompleteTime(), completeTime);
    }

    @Test
    public void testGrandfatheredStartTime() {
        ScanOptions options = new ScanOptions(ImmutableList.of("p0"));
        options.addDestination(ScanDestination.to(URI.create("s3://bucket/path/to/root")));

        ScanRangeStatus pending = new ScanRangeStatus(0, "p0",
                ScanRange.create(ByteBuffer.wrap(new byte[] {0x00}), ByteBuffer.wrap(new byte[] {0x01})),
                0, Optional.of(1), Optional.of(3));

        ScanRangeStatus active = new ScanRangeStatus(1, "p0",
                ScanRange.create(ByteBuffer.wrap(new byte[]{0x02}), ByteBuffer.wrap(new byte[]{0x03})),
                1, Optional.empty(), Optional.empty());
        active.setScanQueuedTime(Date.from(Instant.now().minus(Duration.ofMinutes(10))));
        active.setScanStartTime(Date.from(Instant.now().minus(Duration.ofMinutes(5))));

        ScanRangeStatus complete = new ScanRangeStatus(2, "p0",
                ScanRange.create(ByteBuffer.wrap(new byte[]{0x04}), ByteBuffer.wrap(new byte[]{0x05})),
                2, Optional.empty(), Optional.empty());
        complete.setScanQueuedTime(Date.from(Instant.now().minus(Duration.ofMinutes(30))));
        complete.setScanStartTime(Date.from(Instant.now().minus(Duration.ofMinutes(25))));
        complete.setScanCompleteTime(Date.from(Instant.now().minus(Duration.ofMinutes(20))));

        ScanStatus scanStatus = new ScanStatus("id", options, true, false, new Date(0),
                ImmutableList.of(pending), ImmutableList.of(active), ImmutableList.of(complete));

        _dao.updateScanStatus(scanStatus);

        // Go behind the scenes and remove the "startTime" attribute
        _dataStore.update("scan_table", "id", TimeUUIDs.newUUID(), Deltas.mapBuilder().remove("startTime").build(),
                new AuditBuilder().setComment("Removing startTime").build());

        ScanStatus returned = _dao.getScanStatus("id");
        assertEquals(returned.getStartTime(), complete.getScanQueuedTime());
    }
}
