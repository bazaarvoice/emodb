package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.bazaarvoice.emodb.event.api.BaseEventStore;
import com.bazaarvoice.emodb.queue.core.kafka.KafkaProducerService;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Date;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class DataStoreStashRequestDAOTest {

    private DataStore _dataStore;
    private DataStoreStashRequestDAO _dao;

    @BeforeMethod
    public void setUp() {
        _dataStore = new InMemoryDataStore(new MetricRegistry(), new KafkaProducerService(), mock(BaseEventStore.class));
        _dao = new DataStoreStashRequestDAO(_dataStore, "request_table", "app_global:sys");
    }

    @Test
    public void testGetWithNoRequests() {
        assertTrue(_dao.getRequestsForStash("scan1").isEmpty());
    }

    @Test
    public void testCreateAndUndoRequests() {
        StashRequest request1 = new StashRequest("requester1", new Date(1511297510000L));
        StashRequest request2 = new StashRequest("requester2", new Date(1511297520000L));
        
        _dao.requestStash("scan1", request1);
        assertEquals(_dao.getRequestsForStash("scan1"), ImmutableSet.of(request1));
        _dao.requestStash("scan1", request2);
        assertEquals(_dao.getRequestsForStash("scan1"), ImmutableSet.of(request1, request2));
        _dao.undoRequestStash("scan1", new StashRequest("requester1", new Date(1511297530000L)));
        assertEquals(_dao.getRequestsForStash("scan1"), ImmutableSet.of(request2));
        _dao.undoRequestStash("scan1", new StashRequest("requester2", new Date(1511297540000L)));
        assertEquals(_dao.getRequestsForStash("scan1"), ImmutableSet.of());
    }

    @Test
    public void testMultipleCreateRequestsFromRequester() {
        StashRequest request1 = new StashRequest("requester1", new Date(1511297510000L));
        _dao.requestStash("scan1", request1);
        assertEquals(_dao.getRequestsForStash("scan1"), ImmutableSet.of(request1));
        StashRequest request2 = new StashRequest("requester1", new Date(1511297520000L));
        _dao.requestStash("scan1", request2);
        // Only the latest request should be maintained
        assertEquals(_dao.getRequestsForStash("scan1"), ImmutableSet.of(request2));
    }
}
