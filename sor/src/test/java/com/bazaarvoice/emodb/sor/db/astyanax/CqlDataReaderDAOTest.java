package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.db.DataReaderDAO;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.db.cql.ResultSetSupplier;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.serializers.StringSerializer;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class CqlDataReaderDAOTest {

    @Test
    public void testAscyncResultSupplier() {
        // Verify that ResultSetSupplier eagerly fires asynchronous query if async is true
        Session session = mock(Session.class);
        Statement statement = mock(Statement.class);
        new ResultSetSupplier(session, statement, true);
        verify(session, times(1)).executeAsync(statement);
        verifyNoMoreInteractions(session);
        reset(session, statement);
        new ResultSetSupplier(session, statement, false);
        verifyNoMoreInteractions(session);
    }

    @Test
    public void test() {
        System.out.println(Deltas.mapBuilder()
                .update("ranges", Deltas.mapBuilder()
                        .updateIfExists("-99", Deltas.mapBuilder()
                                .putIfAbsent("completeTime", 1433456)
                                .build())
                        .build())
                .build().toString());
    }

    @Test
    public void rowQueryOutOfOrderIterationOfRecordsShouldNotBeAllowed() {

        CqlDataReaderDAO cqlDataReaderDAO = new CqlDataReaderDAO(mock(DataReaderDAO.class), mock(PlacementCache.class),
                mock(MetricRegistry.class));

        ByteBuffer rowKey1 = StringSerializer.get().toByteBuffer("test1");
        ByteBuffer rowKey2 = StringSerializer.get().toByteBuffer("test2");
        ByteBuffer rowKey3 = StringSerializer.get().toByteBuffer("test3");
        ByteBuffer rowKey4 = StringSerializer.get().toByteBuffer("test4");

        @SuppressWarnings ("unchecked")
        List<Map.Entry<ByteBuffer, Key>> rowKeys = Lists.newArrayList(Maps.immutableEntry(rowKey1, new Key(mock(Table.class), "test1")),
                Maps.immutableEntry(rowKey2, new Key(mock(Table.class), "test2")),
                Maps.immutableEntry(rowKey3, new Key(mock(Table.class), "test3")),
                Maps.immutableEntry(rowKey4, new Key(mock(Table.class), "test4")));

        Row row1 = mock(Row.class);
        Row row2 = mock(Row.class);
        Row row3 = mock(Row.class);
        Row row4 = mock(Row.class);
        when(row1.getBytes(0)).thenReturn(rowKey1);
        when(row2.getBytes(0)).thenReturn(rowKey2);
        when(row3.getBytes(0)).thenReturn(rowKey3);
        when(row4.getBytes(0)).thenReturn(rowKey4);
        List<Row> rows = Lists.newArrayList(row1, row2, row3, row4);

        Iterator<Record> recordsIter = cqlDataReaderDAO.rowQuerySync(rowKeys, ReadConsistency.STRONG, createMockDeltaPlacement(rows));

        // Let us try to resolve these out of order (which simply means traversing the iterators out of order).
        recordsIter.next();

        IllegalStateException exception = null;
        try {
            recordsIter.next().passTwoIterator().next();
            fail();
        } catch (IllegalStateException e) {
            exception = e;
        }
        assertNotNull(exception, "Should throw an IllegalStateException when records iterator is traversed out of order");
    }

    @Test
    public void rowQueryTestHappyPath() {

        CqlDataReaderDAO cqlDataReaderDAO = new CqlDataReaderDAO(mock(DataReaderDAO.class), mock(PlacementCache.class),
                mock(MetricRegistry.class));

        String key1 = "test1";
        String key2 = "test2";
        String key3 = "test3";
        String key4 = "notExists";

        ByteBuffer rowKey1 = StringSerializer.get().toByteBuffer(key1);
        ByteBuffer rowKey2 = StringSerializer.get().toByteBuffer(key2);
        ByteBuffer rowKey3 = StringSerializer.get().toByteBuffer(key3);
        ByteBuffer rowKey4 = StringSerializer.get().toByteBuffer(key4);

        @SuppressWarnings ("unchecked")
        List<Map.Entry<ByteBuffer, Key>> rowKeys = Lists.newArrayList(Maps.immutableEntry(rowKey1, new Key(mock(Table.class), key1)),
                Maps.immutableEntry(rowKey2, new Key(mock(Table.class), key2)),
                Maps.immutableEntry(rowKey3, new Key(mock(Table.class), key3)),
                Maps.immutableEntry(rowKey4, new Key(mock(Table.class), key4)));

        Row row1 = mock(Row.class);
        Row row2 = mock(Row.class);
        Row row3 = mock(Row.class);
        Row row4 = mock(Row.class);
        when(row1.getBytes(0)).thenReturn(rowKey1);
        when(row1.getUUID(1)).thenReturn(TimeUUIDs.newUUID());
        when(row1.getBytesUnsafe(2)).thenReturn(StringSerializer.get().fromString("D1:{..,\"name\":\"Bob\"}"));
        // Second delta for first row
        when(row2.getBytes(0)).thenReturn(rowKey1);
        when(row2.getUUID(1)).thenReturn(TimeUUIDs.newUUID());
        when(row2.getBytesUnsafe(2)).thenReturn(StringSerializer.get().fromString("D1:{..,\"lastname\":\"Bobby\"}"));

        when(row3.getBytes(0)).thenReturn(rowKey2);
        when(row3.getUUID(1)).thenReturn(TimeUUIDs.newUUID());
        when(row3.getBytesUnsafe(2)).thenReturn(StringSerializer.get().fromString("D1:{..,\"size\":\"medium\"}"));

        when(row4.getBytes(0)).thenReturn(rowKey3);
        when(row4.getUUID(1)).thenReturn(TimeUUIDs.newUUID());
        when(row4.getBytesUnsafe(2)).thenReturn(StringSerializer.get().fromString("D1:{..,\"phone\":\"555\"}"));

        List<Row> rows = Lists.newArrayList(row1, row2, row3, row4);

        Iterator<Record> recordsIter = cqlDataReaderDAO.rowQuerySync(rowKeys, ReadConsistency.STRONG, createMockDeltaPlacement(rows));

        // Make sure we get what we expect from Records
        BitSet recordKeyBitSet = new BitSet(rowKeys.size());
        List<String> recordList = Lists.newArrayList(key1, key2, key3, key4);
        Map<String, Integer> keysDeltas = ImmutableMap.of(key1, 2, key2, 1, key3, 1, key4, 0);
        while (recordsIter.hasNext()) {
            Record record = recordsIter.next();
            String key = record.getKey().getKey();
            recordKeyBitSet.set(recordList.indexOf(key));
            assertTrue(Iterators.advance(record.passOneIterator(), 10) == 0, "Compaction found when there were none");
            assertTrue(Iterators.advance(record.passTwoIterator(), 10) == keysDeltas.get(key), "Unexpected number of changes found");
            assertTrue(Iterators.advance(record.rawMetadata(), 10) == keysDeltas.get(key), "Unexpected number of raw metadata entries found");
        }
        assertTrue(recordKeyBitSet.cardinality() == rowKeys.size(), "All records are not accounted for.");

    }

    private DeltaPlacement createMockDeltaPlacement(List<Row> rows) {
        Session mockSession = mock(Session.class);
        ResultSet mockResultSet = mock(ResultSet.class);

        when(mockResultSet.iterator()).thenReturn(rows.iterator());
        when(mockResultSet.isFullyFetched()).thenReturn(true);
        when(mockResultSet.getAvailableWithoutFetching()).thenReturn(rows.size());

        when(mockSession.execute(any(Statement.class))).thenReturn(mockResultSet);

        DeltaPlacement mockDeltaPlacement = mock(DeltaPlacement.class);
        CassandraKeyspace mockKeyspace = mock(CassandraKeyspace.class);
        when(mockKeyspace.getCqlSession()).thenReturn(mockSession);

        KeyspaceMetadata mockKeyspaceMetadata = mock(KeyspaceMetadata.class);
        when(mockKeyspaceMetadata.getName()).thenReturn("ugc_global:ugc");

        when(mockDeltaPlacement.getKeyspace()).thenReturn(mockKeyspace);
        when(mockDeltaPlacement.getDeltaRowKeyColumnName()).thenReturn("rowKey");
        when(mockDeltaPlacement.getDeltaColumnName()).thenReturn("column1");
        when(mockDeltaPlacement.getDeltaValueColumnName()).thenReturn("value");
        TableMetadata mockTableMetadata = mock(TableMetadata.class);
        when(mockTableMetadata.getKeyspace()).thenReturn(mockKeyspaceMetadata);
        when(mockTableMetadata.getName()).thenReturn("ugc_delta");
        when(mockDeltaPlacement.getDeltaTableMetadata()).thenReturn(mockTableMetadata);

        return mockDeltaPlacement;
    }
}
