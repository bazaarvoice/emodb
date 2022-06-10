package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.common.stash.*;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.TableNotStashedException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class DataStoreStashTest {

    @Test
    public void testScan() throws Exception {
        DataStore dataStore = mock(DataStore.class);
        StandardStashReader stashReader = mock(StandardStashReader.class);
        when(stashReader.getLockedView()).thenReturn(stashReader);
        StashRowIterator iterator = mock(StashRowIterator.class);

        when(stashReader.scan("test:table")).thenReturn(iterator);

        List<Map<String, Object>> expectedResults = ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of("~id", "foo", "~table", "test:table"),
                ImmutableMap.<String, Object>of("~id", "bar", "~table", "test:table"));

        final Iterator<Map<String, Object>> actualIterator = expectedResults.iterator();
        when(iterator.hasNext()).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return actualIterator.hasNext();
            }
        });
        when(iterator.next()).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return actualIterator.next();
            }
        });

        DataStoreStash stash = new DataStoreStash(dataStore, stashReader);

        try (StashRowIterable rows = stash.scan("test:table")) {
            List<Map<String, Object>> scanResults = ImmutableList.copyOf(rows);
            assertEquals(expectedResults, scanResults);
        }

        verify(iterator, atLeastOnce()).hasNext();
        verify(iterator, times(2)).next();
        verify(iterator).close();
        verifyNoMoreInteractions(iterator);
    }

    @Test
    public void testScanTableNotStashed() throws Exception {
        DataStore dataStore = mock(DataStore.class);
        StandardStashReader stashReader = mock(StandardStashReader.class);
        when(stashReader.getLockedView()).thenReturn(stashReader);

        when(stashReader.scan("test:table")).thenThrow(new TableNotStashedException("test:table"));
        when(dataStore.getTableExists("test:table")).thenReturn(true);

        DataStoreStash stash = new DataStoreStash(dataStore, stashReader);

        try {
            stash.scan("test:table");
            fail("TableNotStashed not thrown");
        } catch (TableNotStashedException e) {
            // ok
        }
    }

    @Test
    public void testScanUnknownTable() throws Exception {
        DataStore dataStore = mock(DataStore.class);
        StandardStashReader stashReader = mock(StandardStashReader.class);
        when(stashReader.getLockedView()).thenReturn(stashReader);

        when(stashReader.scan("test:table")).thenThrow(new TableNotStashedException("test:table"));

        DataStoreStash stash = new DataStoreStash(dataStore, stashReader);

        try {
            stash.scan("test:table");
            fail("UnknownTableException not thrown");
        } catch (UnknownTableException e) {
            // ok
        }
    }

    @Test
    public void testGetSplits() throws Exception {
        DataStore dataStore = mock(DataStore.class);
        StandardStashReader stashReader = mock(StandardStashReader.class);

        List<StashSplit> stashSplits = Lists.newArrayListWithCapacity(2);
        for (int i=0; i < 2; i++) {
            StashSplit stashSplit = mock(StashSplit.class);
            when(stashSplit.toString()).thenReturn("split" + i);
            stashSplits.add(stashSplit);
        }

        when(stashReader.getSplits("test:table")).thenReturn(stashSplits);

        DataStoreStash stash = new DataStoreStash(dataStore, stashReader);
        Collection<String> actualSplits = stash.getSplits("test:table");

        assertEquals(actualSplits, ImmutableList.of("split0", "split1"));
    }

    @Test
    public void testGetSplit() throws Exception {
        DataStore dataStore = mock(DataStore.class);
        StandardStashReader stashReader = mock(StandardStashReader.class);
        when(stashReader.getLockedView()).thenReturn(stashReader);
        StashRowIterator iterator = mock(StashRowIterator.class);
        String splitString = "MDAxCnRpbHBzdHNldAplbGJhdDp0c2V0";  // Precomputed for table "test:table" and split "testsplit"

        when(stashReader.getSplit(StashSplit.fromString(splitString))).thenReturn(iterator);

        List<Map<String, Object>> expectedResults = ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of("~id", "foo", "~table", "test:table"),
                ImmutableMap.<String, Object>of("~id", "bar", "~table", "test:table"));

        final Iterator<Map<String, Object>> actualIterator = expectedResults.iterator();
        when(iterator.hasNext()).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return actualIterator.hasNext();
            }
        });
        when(iterator.next()).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return actualIterator.next();
            }
        });

        DataStoreStash stash = new DataStoreStash(dataStore, stashReader);

        try (StashRowIterable rows = stash.getSplit("test:table", splitString)) {
            List<Map<String, Object>> scanResults = ImmutableList.copyOf(rows);
            assertEquals(expectedResults, scanResults);
        }

        verify(iterator, atLeastOnce()).hasNext();
        verify(iterator, times(2)).next();
        verify(iterator).close();
        verifyNoMoreInteractions(iterator);
    }

    @Test
    public void testListStashTables() throws Exception {
        DataStore dataStore = mock(DataStore.class);
        StandardStashReader stashReader = mock(StandardStashReader.class);
        when(stashReader.getLockedView()).thenReturn(stashReader);

        when(stashReader.listTables()).thenReturn(
                ImmutableList.of(new StashTable("bucket", "stash/table/foo/", "foo")).iterator());

        DataStoreStash stash = new DataStoreStash(dataStore, stashReader);
        List<StashTable> stashTables = ImmutableList.copyOf(stash.listStashTables());

        assertEquals(stashTables, ImmutableList.of(new StashTable("bucket", "stash/table/foo/", "foo")));
        verify(stashReader).getLockedView();
        verify(stashReader).listTables();
        verifyNoMoreInteractions(stashReader);
    }

    @Test
    public void testListStashTableNames() throws Exception {
        DataStore dataStore = mock(DataStore.class);
        StandardStashReader stashReader = mock(StandardStashReader.class);
        when(stashReader.getLockedView()).thenReturn(stashReader);

        when(stashReader.listTables()).thenReturn(
                ImmutableList.of(new StashTable("bucket", "stash/table/foo/", "foo")).iterator());

        DataStoreStash stash = new DataStoreStash(dataStore, stashReader);
        List<String> stashTableNames = ImmutableList.copyOf(stash.listStashTableNames());

        assertEquals(stashTableNames, ImmutableList.of("foo"));
        verify(stashReader).getLockedView();
        verify(stashReader).listTables();
        verifyNoMoreInteractions(stashReader);
    }

    @Test
    public void testListStashTableMetadata() throws Exception {
        DataStore dataStore = mock(DataStore.class);
        StandardStashReader stashReader = mock(StandardStashReader.class);
        when(stashReader.getLockedView()).thenReturn(stashReader);

        when(stashReader.listTableMetadata()).thenReturn(ImmutableList.of(
                new StashTableMetadata("bucket", "stash/table/foo/", "foo", ImmutableList.of(
                        new StashFileMetadata("bucket", "stash/table/foo/foo1.json.gz", 100)))
                ).iterator());

        DataStoreStash stash = new DataStoreStash(dataStore, stashReader);
        List<StashTableMetadata> stashTableMetadata = ImmutableList.copyOf(stash.listStashTableMetadata());

        assertEquals(stashTableMetadata, ImmutableList.of(
                new StashTableMetadata("bucket", "stash/table/foo/", "foo", ImmutableList.of(
                        new StashFileMetadata("bucket", "stash/table/foo/foo1.json.gz", 100)))));
        verify(stashReader).getLockedView();
        verify(stashReader).listTableMetadata();
        verifyNoMoreInteractions(stashReader);
    }
}
