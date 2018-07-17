package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.db.astyanax.AstyanaxDataReaderDAO;
import com.bazaarvoice.emodb.sor.db.astyanax.CqlDataReaderDAO;
import com.datastax.driver.core.Row;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.netflix.astyanax.model.Column;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class MultiScanCutoffTimeTest {

    private static final String KEY = "key1";

    @Test
    public void testCQLRowFilteringBasedOnCutoffTime()
            throws Exception {
        long nowInTimeMillis = System.currentTimeMillis();

        UUID uuid1 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis);
        UUID uuid2 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 5000);
        UUID uuid3 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 10000);
        UUID uuid4 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 15000);
        UUID uuid5 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 20000);

        Row row1 = cqlRow(KEY, uuid1, "a");
        Row row2 = cqlRow(KEY, uuid2, "b");
        Row row3 = cqlRow(KEY, uuid3, "c");
        Row row4 = cqlRow(KEY, uuid4, "d");
        Row row5 = cqlRow(KEY, uuid5, "e");

        final Iterable<Row> rows = Arrays.asList(row1, row2, row3, row4, row5);
        assertEquals(Iterables.size(rows), 5);

        Iterable<Row> filteredRows = CqlDataReaderDAO.getFilteredRows(rows, null);
        assertEquals(Iterables.size(filteredRows), 5);

        filteredRows = CqlDataReaderDAO.getFilteredRows(rows, Instant.ofEpochMilli(nowInTimeMillis - 1000));
        assertEquals(Iterables.size(filteredRows), 0);

        filteredRows = CqlDataReaderDAO.getFilteredRows(rows, Instant.ofEpochMilli(nowInTimeMillis + 12000));
        assertEquals(Iterables.size(filteredRows), 3);
        assertEquals(Iterables.get(filteredRows, 0).getString(2), "a");
        assertEquals(Iterables.get(filteredRows, 1).getString(2), "b");
        assertEquals(Iterables.get(filteredRows, 2).getString(2), "c");

        filteredRows = CqlDataReaderDAO.getFilteredRows(rows, Instant.ofEpochMilli(nowInTimeMillis + 21000));
        assertEquals(Iterables.size(filteredRows), 5);
    }


    @Test
    public void testAstyanaxColumnFilteringBasedOnCutoffTime()
            throws Exception {
        long nowInTimeMillis = System.currentTimeMillis();

        UUID uuid1 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis);
        UUID uuid2 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 5000);
        UUID uuid3 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 10000);
        UUID uuid4 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 15000);
        UUID uuid5 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 20000);

        Column<UUID> col1 = astyanaxColumn(uuid1, "a");
        Column<UUID> col2 = astyanaxColumn(uuid2, "b");
        Column<UUID> col3 = astyanaxColumn(uuid3, "c");
        Column<UUID> col4 = astyanaxColumn(uuid4, "d");
        Column<UUID> col5 = astyanaxColumn(uuid5, "e");

        final Iterable<Column<UUID>> columns = Arrays.asList(col1, col2, col3, col4, col5);
        assertEquals(Iterators.size(columns.iterator()), 5);

        Iterator<Column<UUID>> filteredColumnIter = AstyanaxDataReaderDAO.getFilteredColumnIter(columns.iterator(), null);
        assertEquals(Iterators.size(filteredColumnIter), 5);

        filteredColumnIter = AstyanaxDataReaderDAO.getFilteredColumnIter(columns.iterator(), Instant.ofEpochMilli(nowInTimeMillis - 1000));
        assertEquals(Iterators.size(filteredColumnIter), 0);

        filteredColumnIter = AstyanaxDataReaderDAO.getFilteredColumnIter(columns.iterator(), Instant.ofEpochMilli(nowInTimeMillis + 12000));
        assertEquals(Iterators.size(filteredColumnIter), 3);

        filteredColumnIter = AstyanaxDataReaderDAO.getFilteredColumnIter(columns.iterator(), Instant.ofEpochMilli(nowInTimeMillis + 21000));
        assertEquals(Iterators.size(filteredColumnIter), 5);
    }

    private Row cqlRow(String key, UUID column, String value) {
        Row row = mock(Row.class);
        when(row.getString(0)).thenReturn(key);
        when(row.getUUID(1)).thenReturn(column);
        when(row.getString(2)).thenReturn(value);

        return row;
    }

    private Column<UUID> astyanaxColumn(UUID uuidValue, String value) {
        Column<UUID> column = mock(Column.class);
        when(column.getUUIDValue()).thenReturn(uuidValue);
        when(column.getStringValue()).thenReturn(value);

        return column;
    }

}