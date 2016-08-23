package com.bazaarvoice.emodb.sor.db.cql;

import com.datastax.driver.core.Row;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.ref.SoftReference;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class CachingRowGroupIteratorTest {

    private Queue<RowGroup> _rowGroups;
    private Table<String, String, SoftReference<List<Row>>> _softReferences;
    private List<Row> _reloadedRows;

    @BeforeMethod
    public void setUp() {
        _rowGroups = new ArrayDeque<>();
        _softReferences = HashBasedTable.create();
    }

    @Test
    public void testSingleRowCached() throws Exception {
        addRowGroup(row("a", "b", "c"));
        CachingRowGroupIterator iterator = new CachingRowGroupIteratorWithMockSoftReferences(_rowGroups.iterator(), 20, 10);
        assertTrue(iterator.hasNext());
        // Get the rows from the row group
        Iterable<Row> rows = iterator.next();
        for (int i=0; i < 2; i++) {
            assertTrue(groupsEqual(rows, row("a", "b", "c")));
            // No records should have been added to the soft cache
            assertTrue(_softReferences.isEmpty());
        }
    }

    @Test
    public void testMaxCacheRowsCached() throws Exception {
        addRowGroup(row("a", "1", "1"), row("a", "2", "2"), row("a", "3", "3"));

        CachingRowGroupIterator iterator = new CachingRowGroupIteratorWithMockSoftReferences(_rowGroups.iterator(), 3, 10);
        assertTrue(iterator.hasNext());
        // Get the rows from the row group
        Iterable<Row> rows = iterator.next();
        for (int i=0; i < 2; i++) {
            assertTrue(groupsEqual(rows, row("a", "1", "1"), row("a", "2", "2"), row("a", "3", "3")));
            // No records should have been added to the soft cache
            assertTrue(_softReferences.isEmpty());
        }
    }

    @Test
    public void testOneRowSoftCached() throws Exception {
        addRowGroup(row("a", "1", "1"), row("a", "2", "2"), row("a", "3", "3"));

        CachingRowGroupIterator iterator = new CachingRowGroupIteratorWithMockSoftReferences(_rowGroups.iterator(), 2, 10);
        assertTrue(iterator.hasNext());
        // Get the rows from the row group
        Iterable<Row> rows = iterator.next();
        for (int i=0; i < 2; i++) {
            assertTrue(groupsEqual(rows, row("a", "1", "1"), row("a", "2", "2"), row("a", "3", "3")));
            // Last row should have been put in the soft cache
            assertEquals(_softReferences.size(), 1);
            assertTrue(_softReferences.contains("a", "3"));
        }
    }

    @Test
    public void testReloadOnSoftCacheMissing() throws Exception {
        addRowGroup(row("a", "1", "1"), row("a", "2", "2"), row("a", "3", "3"), row("a", "4", "4"),
                row("a", "5", "5"), row("a", "6", "6"), row("a", "7", "7"), row("a", "8", "8"));

        CachingRowGroupIterator iterator = new CachingRowGroupIteratorWithMockSoftReferences(_rowGroups.iterator(), 2, 2);
        assertTrue(iterator.hasNext());
        // Get the rows from the row group
        Iterable<Row> rows = iterator.next();
        assertTrue(groupsEqual(rows, row("a", "1", "1"), row("a", "2", "2"), row("a", "3", "3"),
                row("a", "4", "4"), row("a", "5", "5"), row("a", "6", "6"), row("a", "7", "7"), row("a", "8", "8")));
        // Last 6 rows should have been put in the soft cache
        assertEquals(_softReferences.size(), 3);
        assertTrue(_softReferences.contains("a", "3"));
        assertTrue(_softReferences.contains("a", "5"));
        assertTrue(_softReferences.contains("a", "7"));

        // Simulate [a,5,5], the second row group in the soft cache, being garbage collected
        //noinspection unchecked
        reset(_softReferences.get("a", "5"));

        // Iterate again
        assertTrue(groupsEqual(rows, row("a", "1", "1"), row("a", "2", "2"), row("a", "3", "3"),
                row("a", "4", "4"), row("a", "5", "5"), row("a", "6", "6"), row("a", "7", "7"), row("a", "8", "8")));

        // This time rows 5 through 8 should have come from a reload
        assertTrue(groupsEqual(_reloadedRows, row("a", "5", "5"), row("a", "6", "6"), row("a", "7", "7"), row("a", "8", "8")));
    }

    private class CachingRowGroupIteratorWithMockSoftReferences extends CachingRowGroupIterator {
        private CachingRowGroupIteratorWithMockSoftReferences(Iterator<RowGroup> rowGroupIterator, int maxCacheSize,
                                                              int softCacheGroupSize) {
            super(rowGroupIterator, maxCacheSize, softCacheGroupSize);
        }

        @Override
        protected SoftReference<List<Row>> softlyReferenced(List<Row> rows) {
            //noinspection unchecked
            SoftReference<List<Row>> ref = mock(SoftReference.class);
            // Record this list was returned in a soft reference, keyed by the first record
            _softReferences.put(rows.get(0).getString(0), rows.get(0).getString(1), ref);
            // By default the reference will return the rows.  Callers can reset the mock to override this behavior.
            when(ref.get()).thenReturn(rows);
            return ref;
        }
    }

    private Row row(String key, String column, String value) {
        Row row = mock(Row.class);
        when(row.getString(0)).thenReturn(key);
        when(row.getString(1)).thenReturn(column);
        when(row.getString(2)).thenReturn(value);

        return row;
    }

    private RowGroup addRowGroup(final Row... rows) {
        final Iterator<Row> rowIterator = Arrays.asList(rows).iterator();

        RowGroup rowGroup = mock(RowGroup.class);
        when(rowGroup.hasNext()).thenAnswer(
                new Answer<Boolean>() {
                    @Override
                    public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
                        return rowIterator.hasNext();
                    }
                });

        when(rowGroup.next()).thenAnswer(
                new Answer<Row>() {
                    @Override
                    public Row answer(InvocationOnMock invocationOnMock) throws Throwable {
                        return rowIterator.next();
                    }
                });

        when(rowGroup.reloadRowsAfter(any(Row.class))).thenAnswer(
                new Answer<Object>() {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                        Row after = (Row) invocationOnMock.getArguments()[0];

                        for (int i=0; i < rows.length; i++) {
                            if (rowsEqual(after, rows[i])) {
                                _reloadedRows = Arrays.asList(rows).subList(i+1, rows.length);
                                return _reloadedRows.iterator();
                            }
                        }

                        fail("After row not found in results");
                        // unreachable
                        return null;
                    }
                });

        _rowGroups.add(rowGroup);
        return rowGroup;
    }

    private boolean groupsEqual(Iterable<Row> rows, Row... expected) {
        Iterator<Row> expectedIterator = Arrays.asList(expected).iterator();
        for (Row row : rows) {
            if (!expectedIterator.hasNext() || !rowsEqual(row, expectedIterator.next())) {
                return false;
            }
        }
        return !expectedIterator.hasNext();
    }

    private boolean rowsEqual(Row l, Row r) {
        return l.getString(0).equals(r.getString(0)) &&
            l.getString(1).equals(r.getString(1)) &&
            l.getString(2).equals(r.getString(2));
    }
}
