package com.bazaarvoice.emodb.auth.shiro;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.google.common.cache.Cache;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;

public class GuavaCacheManagerTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testConcurrentCacheRegistration() throws Exception {
        CacheRegistry cacheRegistry = mock(CacheRegistry.class);
        when(cacheRegistry.register(eq("cacheName"), any(Cache.class), eq(true)))
                .thenReturn(mock(CacheHandle.class))
                .thenThrow(new IllegalStateException("Multiple calls to register same cache"));

        final GuavaCacheManager cacheManager = new GuavaCacheManager(cacheRegistry);
        List<Future<org.apache.shiro.cache.Cache>> futures = Lists.newArrayListWithCapacity(2);
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        final int threadCount = 10;

        try {
            // Create multiple threads that attempt to register the cache concurrently
            final CountDownLatch latch = new CountDownLatch(threadCount);

            for (int i=0; i < threadCount; i++) {
                Callable<org.apache.shiro.cache.Cache> registerCache = new Callable<org.apache.shiro.cache.Cache>() {
                    @Override
                    public org.apache.shiro.cache.Cache call() throws Exception {
                        // Use the latch to get them to create the cache as close to concurrently as possible.
                        // This isn't guaranteed to cause call concurrency but there's no hooks possible to
                        // make this deterministic.
                        latch.countDown();
                        latch.await();
                        return cacheManager.createCache("cacheName");
                    }
                };

                futures.add(executorService.submit(registerCache));
            }

            // Verify the same cache instance was returned by all threads
            for (int i=1; i < threadCount; i++) {
                assertSame(futures.get(0).get(10, TimeUnit.SECONDS), futures.get(i).get(10, TimeUnit.SECONDS));
            }
            verify(cacheRegistry, times(1)).register(eq("cacheName"), any(Cache.class), eq(true));
        } finally {
            executorService.shutdownNow();
        }
    }
}