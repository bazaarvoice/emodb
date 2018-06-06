package com.bazaarvoice.emodb.table.db.generic;

import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.google.common.cache.LoadingCache;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

public class CachingTableDAOTest {

    private TableDAO _delegate;
    private Instant _now;
    private CachingTableDAO _cachingTableDAO;
    private LoadingCache _cache;

    @BeforeMethod
    public void setUp() {
        _delegate = mock(TableDAO.class);

        _now = Instant.ofEpochMilli(1526068800000L);
        Clock clock = mock(Clock.class);
        when(clock.instant()).then(ignore -> _now);
        when(clock.millis()).then(ignore -> _now.toEpochMilli());

        CacheRegistry cacheRegistry = mock(CacheRegistry.class);
        _cachingTableDAO = new CachingTableDAO(_delegate, cacheRegistry, clock);

        ArgumentCaptor<LoadingCache> argumentCaptor = ArgumentCaptor.forClass(LoadingCache.class);
        verify(cacheRegistry).register(eq("tables"), argumentCaptor.capture(), eq(true));
        _cache = argumentCaptor.getValue();
    }

    @AfterMethod
    public void verifyMocks() {
        verifyNoMoreInteractions(_delegate);
    }

    @Test
    public void testTableCached() throws Exception {
        Table firstResponse = mock(Table.class);
        Table secondResponse = mock(Table.class);

        when(_delegate.get("table")).thenReturn(firstResponse, secondResponse);
        Table table = _cachingTableDAO.get("table");
        assertSame(table, firstResponse);

        // Move time forward one minute, should still get back cached instance
        _now = _now.plus(Duration.ofMinutes(1));
        table = _cachingTableDAO.get("table");
        assertSame(table, firstResponse);

        // 1 millisecond prior to cache invalidation
        _now = _now.plus(Duration.ofMinutes(9).minusMillis(1));
        table = _cachingTableDAO.get("table");
        assertSame(table, firstResponse);

        // 1 millisecond after cache invalidation
        _now = _now.plus(Duration.ofMillis(2));
        table = _cachingTableDAO.get("table");
        assertSame(table, secondResponse);

        verify(_delegate, times(2)).get("table");
    }

    @Test
    public void testTableInvalidation() throws Exception {
        Table firstResponse = mock(Table.class);
        Table secondResponse = mock(Table.class);

        when(_delegate.get("table")).thenReturn(firstResponse, secondResponse);
        Table table = _cachingTableDAO.get("table");
        assertSame(table, firstResponse);

        _cache.invalidateAll();
        table = _cachingTableDAO.get("table");
        assertSame(table, secondResponse);

        verify(_delegate, times(2)).get("table");
    }

    @Test
    public void testShortCachingOfUnknownTable() throws Exception {
        Table actualTable = mock(Table.class);

        when(_delegate.get("table"))
                .thenThrow(new UnknownTableException())
                .thenReturn(actualTable);

        try {
            _cachingTableDAO.get("table");
            fail("Unknown table");
        } catch (UnknownTableException e) {
            // expected
        }

        // Move just under 2 seconds, should still get back cached exception
        _now = _now.plus(Duration.ofSeconds(2).minusMillis(1));
        try {
            _cachingTableDAO.get("table");
            fail("Unknown table");
        } catch (UnknownTableException e) {
            // expected
        }

        // Move to just over 2 seconds, should re-read the table from delegate
        _now = _now.plus(Duration.ofMillis(2));
        Table table = _cachingTableDAO.get("table");
        assertSame(table, actualTable);

        verify(_delegate, times(2)).get("table");
    }
}
