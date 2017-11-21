package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Date;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class DataStoreScanRequestDAOTest {

    private DataStore _dataStore;
    private DataStoreScanRequestDAO _dao;

    @BeforeMethod
    public void setUp() {
        _dataStore = new InMemoryDataStore(new MetricRegistry());
        _dao = new DataStoreScanRequestDAO(_dataStore, "request_table", "app_global:sys");
    }

    @Test
    public void testGetWithNoRequests() {
        assertTrue(_dao.getRequestsForScan("scan1").isEmpty());
    }

    @Test
    public void testCreateAndUndoRequests() {
        ScanRequest request1 = new ScanRequest("requester1", new Date(1511297510000L));
        ScanRequest request2 = new ScanRequest("requester2", new Date(1511297520000L));
        
        _dao.requestScan("scan1", request1);
        assertEquals(_dao.getRequestsForScan("scan1"), ImmutableSet.of(request1));
        _dao.requestScan("scan1", request2);
        assertEquals(_dao.getRequestsForScan("scan1"), ImmutableSet.of(request1, request2));
        _dao.undoRequestScan("scan1", new ScanRequest("requester1", new Date(1511297530000L)));
        assertEquals(_dao.getRequestsForScan("scan1"), ImmutableSet.of(request2));
        _dao.undoRequestScan("scan1", new ScanRequest("requester2", new Date(1511297540000L)));
        assertEquals(_dao.getRequestsForScan("scan1"), ImmutableSet.of());
    }

    @Test
    public void testMultipleCreateRequestsFromRequester() {
        ScanRequest request1 = new ScanRequest("requester1", new Date(1511297510000L));
        _dao.requestScan("scan1", request1);
        assertEquals(_dao.getRequestsForScan("scan1"), ImmutableSet.of(request1));
        ScanRequest request2 = new ScanRequest("requester1", new Date(1511297520000L));
        _dao.requestScan("scan1", request2);
        // Only the latest request should be maintained
        assertEquals(_dao.getRequestsForScan("scan1"), ImmutableSet.of(request2));
    }
}
