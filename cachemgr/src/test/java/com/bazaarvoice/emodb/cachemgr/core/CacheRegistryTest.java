package com.bazaarvoice.emodb.cachemgr.core;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationEvent;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.cachemgr.invalidate.RemoteInvalidationListener;
import com.bazaarvoice.emodb.cachemgr.invalidate.RemoteInvalidationProvider;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.codahale.metrics.MetricRegistry;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class CacheRegistryTest {
    @Test
    public void testListenerExceptionsPropagate() {
        CacheRegistry registry = new DefaultCacheRegistry(new SimpleLifeCycleRegistry(), new MetricRegistry());

        final AtomicBoolean sameDcCalled = new AtomicBoolean();
        new RemoteInvalidationListener(registry, new RemoteInvalidationProvider() {
            @Override
            public void invalidateOtherServersInSameDataCenter(InvalidationEvent event) {
                sameDcCalled.set(true);
            }

            @Override
            public void invalidateOtherDataCenters(InvalidationEvent event) {
                throw new RuntimeException("Data center unavailable");
            }
        });

        CacheHandle handle = registry.lookup("test-cache", true);

        handle.invalidate(InvalidationScope.LOCAL, "test-key");
        assertFalse(sameDcCalled.get());

        handle.invalidate(InvalidationScope.DATA_CENTER, "test-key");
        assertTrue(sameDcCalled.get());

        try {
            handle.invalidate(InvalidationScope.GLOBAL, "test-key");
            fail();
        } catch (RuntimeException e) {
            assertEquals(e.getMessage(), "Data center unavailable");
        }
    }
}
