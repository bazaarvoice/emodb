package com.bazaarvoice.emodb.sor.db.cql;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.Futures;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.Queue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class RowGroupResultSetIteratorTest {

    private RowGroupResultSetIterator _iterator;
    private Queue<Row> _rows;

    @BeforeMethod
    public void setUp() {
        _rows = new ArrayDeque<>();

        ResultSet resultSet = mock(ResultSet.class);

        when(resultSet.isExhausted()).thenAnswer(
                new Answer<Boolean>() {
                    @Override
                    public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
                        return _rows.isEmpty();
                    }
                });

        when(resultSet.getAvailableWithoutFetching()).thenAnswer(
                new Answer<Integer>() {
                    @Override
                    public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
                        return _rows.size();
                    }
                });

        when(resultSet.one()).thenAnswer(
                new Answer<Row>() {
                    @Override
                    public Row answer(InvocationOnMock invocationOnMock) throws Throwable {
                        return _rows.poll();
                    }
                });

        when(resultSet.fetchMoreResults()).thenReturn(Futures.<ResultSet>immediateFuture(null));
        when(resultSet.isFullyFetched()).thenReturn(true);

        _iterator = new RowGroupResultSetIterator(resultSet, 10) {
            @Override
            protected Object getKeyForRow(Row row) {
                // Key is the first column
                return row.getString(0);
            }

            @Override
            protected ResultSet queryRowGroupRowsAfter(Row row) {
                // Not used in this test
                return null;
            }
        };
    }

    @Test
    public void testNoRows() throws Exception {
        assertFalse(_iterator.hasNext(), "Iterator should contain no results");
    }

    @Test
    public void testSingleGroupSingleRow() throws Exception {
        addRow("a", "b", "c");
        assertTrue(_iterator.hasNext());
        assertRowGroupOf(_iterator.next(), row("a", "b", "c"));
        assertFalse(_iterator.hasNext());
    }

    @Test
    public void testSingleGroupMultipleRows() throws Exception {
        Row[] expected = new Row[50];
        for (int i=0; i < 50; i++) {
            addRow("a", "b" + i, "c" + i);
            expected[i] = row("a", "b" + i, "c" + i);
        }

        assertTrue(_iterator.hasNext());
        assertRowGroupOf(_iterator.next(), expected);
        assertFalse(_iterator.hasNext());
    }

    @Test
    public void testMultipleGroupsSingleRow() throws Exception {
        Row[] expected = new Row[50];
        for (int i=0; i < 50; i++) {
            addRow("a" + i, "b" + i, "c" + i);
            expected[i] = row("a" + i, "b" + i, "c" + i);
        }

        for (int i=0; i < 50; i++) {
            assertTrue(_iterator.hasNext());
            assertRowGroupOf(_iterator.next(), expected[i]);
        }
        assertFalse(_iterator.hasNext());
    }

    @Test
    public void testMultipleGroupsMultipleRows() throws Exception {
        Row[][] expected = new Row[10][50];
        for (int i=0; i < 10; i++) {
            for (int j=0; j < 50; j++) {
                addRow("a" + i, "b" + j, "c" + j);
                expected[i][j] = row("a" + i, "b" + j, "c" + j);
            }
        }

        for (int i=0; i < 10; i++) {
            assertTrue(_iterator.hasNext());
            assertRowGroupOf(_iterator.next(), expected[i]);
        }
        assertFalse(_iterator.hasNext());
    }

    @Test
    public void testCannotReadGroupsOutOfOrder() throws Exception {
        addRow("a", "b", "c");
        addRow("a", "d", "e");
        addRow("f", "g", "h");

        assertTrue(_iterator.hasNext());
        RowGroup a = _iterator.next();

        // The following call should fail because not all rows from RowGroup "a" have been iterated over.
        try {
            _iterator.hasNext();
            fail();
        } catch (IllegalStateException e) {
            // Expected
        }
    }

    private Row row(String key, String column, String value) {
        Row row = mock(Row.class);
        when(row.getString(0)).thenReturn(key);
        when(row.getString(1)).thenReturn(column);
        when(row.getString(2)).thenReturn(value);

        return row;
    }

    private void addRow(String key, String column, String value) {
        _rows.add(row(key, column, value));
    }

    private void assertRowGroupOf(RowGroup rowGroup, Row... rows) {
        for (Row row : rows) {
            assertTrue(rowGroup.hasNext());
            Row next = rowGroup.next();
            assertEquals(next.getString(0), row.getString(0));
            assertEquals(next.getString(1), row.getString(1));
            assertEquals(next.getString(2), row.getString(2));
        }

        assertFalse(rowGroup.hasNext());
    }
}
