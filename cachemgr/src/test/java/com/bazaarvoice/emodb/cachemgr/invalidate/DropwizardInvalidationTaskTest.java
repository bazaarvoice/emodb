package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.google.common.collect.ImmutableMultimap;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class DropwizardInvalidationTaskTest {

    @Test
    public void testLocalKey() throws Exception {
        ImmutableMultimap<String, String> request = ImmutableMultimap.of(
                "cache", "tables",
                "scope", "local",
                "key", "123");
        verifyInvalidate(request, "tables", InvalidationScope.LOCAL, Arrays.asList("123"));
    }

    @Test
    public void testDataCenterAll() throws Exception {
        ImmutableMultimap<String, String> request = ImmutableMultimap.of(
                "cache", "locations",
                "scope", "data_center"
        );
        verifyInvalidate(request, "locations", InvalidationScope.DATA_CENTER, null);
    }

    @Test
    public void testGlobalKeys() throws Exception {
        ImmutableMultimap<String, String> request = ImmutableMultimap.of(
                "cache", "tables",
                "scope", "local",
                "key", "123",
                "key", "456");
        verifyInvalidate(request, "tables", InvalidationScope.LOCAL, Arrays.asList("123", "456"));
    }

    private void verifyInvalidate(ImmutableMultimap<String, String> request,
                                  String name, InvalidationScope scope, @Nullable List<String> keys) throws Exception {
        TaskRegistry tasks = mock(TaskRegistry.class);
        CacheRegistry cacheRegistry = mock(CacheRegistry.class);
        CacheHandle cacheHandle = mock(CacheHandle.class);
        when(cacheRegistry.lookup(name, false)).thenReturn(cacheHandle);
        StringWriter buf = new StringWriter();

        DropwizardInvalidationTask task = new DropwizardInvalidationTask(tasks, cacheRegistry);
        task.execute(request, new PrintWriter(buf));

        assertEquals(buf.toString(), format("Done!%n"));
        if (keys == null) {
            verify(cacheHandle).invalidateAll(scope);
        } else {
            verify(cacheHandle).invalidateAll(scope, keys);
        }

        verify(tasks).addTask(task);
    }

    @Test
    public void testUnknownCache() throws Exception {
        ImmutableMultimap<String, String> request = ImmutableMultimap.of(
                "cache", "bogus",
                "scope", "local",
                "key", "123");

        CacheRegistry cacheRegistry = mock(CacheRegistry.class);
        when(cacheRegistry.lookup("bogus", false)).thenReturn(null);
        StringWriter buf = new StringWriter();

        DropwizardInvalidationTask task = new DropwizardInvalidationTask(mock(TaskRegistry.class), cacheRegistry);
        task.execute(request, new PrintWriter(buf));

        assertEquals(buf.toString(), format("Cache not found: bogus%n"));
    }
}
