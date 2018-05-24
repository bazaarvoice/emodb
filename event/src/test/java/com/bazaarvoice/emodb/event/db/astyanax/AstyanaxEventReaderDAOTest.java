package com.bazaarvoice.emodb.event.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.ColumnCountQuery;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class AstyanaxEventReaderDAOTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testCountCleanupOldOpenSlab() throws Exception {
        // Test EventReaderDAO.getCount() when the slab to read is old but still open.
        final ByteBuffer slabId = TimeUUIDSerializer.get().toByteBuffer(TimeUUIDs.minimumUuid());
        final boolean open = true;
        final int count = 0;

        CassandraKeyspace cassandraKeyspace = mock(CassandraKeyspace.class);
        when(cassandraKeyspace.prepareQuery(Matchers.<ColumnFamily<String, ByteBuffer>>any(), Matchers.<ConsistencyLevel>any()))
                // 'manifest' query
                .then(new Answer<Object>() {
                    @Override
                    public Object answer(InvocationOnMock invocation) throws Throwable {
                        // Return a single open column
                        Column<ByteBuffer> column = mock(Column.class);
                        when(column.getName()).thenReturn(slabId);
                        when(column.getBooleanValue()).thenReturn(open);

                        ColumnList<ByteBuffer> list1 = mock(ColumnList.class);
                        when(list1.iterator()).thenReturn(Iterators.singletonIterator(column));
                        OperationResult<ColumnList<ByteBuffer>> result1 = mock(OperationResult.class);
                        when(result1.getResult()).thenReturn(list1);

                        ColumnList<ByteBuffer> list2 = mock(ColumnList.class);
                        when(list2.isEmpty()).thenReturn(true);
                        OperationResult<ColumnList<ByteBuffer>> result2 = mock(OperationResult.class);
                        when(result2.getResult()).thenReturn(list2);

                        RowQuery<String, ByteBuffer> rowQuery = mock(RowQuery.class);
                        when(rowQuery.withColumnRange(Matchers.<ByteBufferRange>any())).thenReturn(rowQuery);
                        when(rowQuery.autoPaginate(Matchers.anyBoolean())).thenReturn(rowQuery);
                        when(rowQuery.execute())
                                .thenReturn(result1)  // first page
                                .thenReturn(result2); // second page

                        ColumnFamilyQuery<String, ByteBuffer> cfQuery = mock(ColumnFamilyQuery.class);
                        when(cfQuery.getKey(Matchers.<String>any())).thenReturn(rowQuery);

                        return (ColumnFamilyQuery) cfQuery;
                    }
                })
                // 'slab' query
                .then(new Answer<Object>() {
                    @Override
                    public Object answer(InvocationOnMock invocation) throws Throwable {
                        OperationResult<Integer> result = mock(OperationResult.class);
                        when(result.getResult()).thenReturn(count);

                        ColumnCountQuery countQuery = mock(ColumnCountQuery.class);
                        when(countQuery.execute()).thenReturn(result);

                        RowQuery<ByteBuffer, Integer> rowQuery = mock(RowQuery.class);
                        when(rowQuery.withColumnRange(0, Constants.OPEN_SLAB_MARKER - 1, false, Integer.MAX_VALUE)).thenReturn(rowQuery);
                        when(rowQuery.getCount()).thenReturn(countQuery);

                        ColumnFamilyQuery<ByteBuffer, Integer> cfQuery = mock(ColumnFamilyQuery.class);
                        when(cfQuery.getKey(Matchers.<ByteBuffer>any())).thenReturn(rowQuery);

                        return (ColumnFamilyQuery) cfQuery;
                    }
                });

        // Mutations are not allowed!  Fail if async deletion gets scheduled.
        ExecutorService executorService = mock(ExecutorService.class);
        when(executorService.submit(Matchers.<Runnable>any())).thenThrow(AssertionError.class);

        AstyanaxEventReaderDAO readerDao = new AstyanaxEventReaderDAO(
                cassandraKeyspace, mock(ManifestPersister.class), "metricsGroup", executorService, new MetricRegistry());

        // Execute the test
        long result = readerDao.count("channel", 10);

        assertEquals(result, count);
    }

    @Test
    public void testSlabFilterSince() {
        // Test that SlabFilter returns the correct slabs to read if we are only interested in
        // events after a certain time
        Instant now = Instant.now();
        final MetricRegistry metricRegistry = new MetricRegistry();
        TimeUUIDSerializer serializer = TimeUUIDSerializer.get();
        UUID slabId1 = TimeUUIDs.uuidForTimeMillis(now.minus(Duration.ofHours(6)).toEpochMilli());
        UUID slabId2 = TimeUUIDs.uuidForTimeMillis(now.minus(Duration.ofHours(5)).toEpochMilli());
        UUID slabId3 = TimeUUIDs.uuidForTimeMillis(now.minus(Duration.ofHours(4)).toEpochMilli());
        UUID slabId4 = TimeUUIDs.uuidForTimeMillis(now.minus(Duration.ofHours(3)).toEpochMilli());
        UUID slabId5 = TimeUUIDs.uuidForTimeMillis(now.minus(Duration.ofHours(2)).toEpochMilli());

        List<UUID> orderedSlabIds = Lists.newArrayList(slabId1, slabId2, slabId3, slabId4, slabId5);
        // We are only interested in slabs that *may* contain events on or after 'since' date
        Date since = Date.from(now.minus(Duration.ofHours(2)));
        AstyanaxEventReaderDAO eventReaderDAO = new AstyanaxEventReaderDAO(
                mock(CassandraKeyspace.class), mock(ManifestPersister.class), "metricsGroup", mock(ExecutorService.class), metricRegistry);
        SlabFilter slabFilterSince = eventReaderDAO.getSlabFilterSince(since, "testchannel");

        for(int i = 0; i < orderedSlabIds.size(); i++) {
            UUID currSlabId = orderedSlabIds.get(i);
            ByteBuffer slabId = serializer.toByteBuffer(currSlabId);
            ByteBuffer nextSlabId = (i + 1 < orderedSlabIds.size()) ? serializer.toByteBuffer(orderedSlabIds.get(i + 1))
                    : null;
            boolean actual = slabFilterSince.accept(slabId, false, nextSlabId);
            // Slabs slabId4 and slabId5 are the only interesting ones for us
            boolean expected = currSlabId.equals(slabId4) || currSlabId.equals(slabId5);
            assertEquals(actual, expected, "slabId4 and slabId5 are the only ones we care about.");
        }
    }
}
