package com.bazaarvoice.emodb.event.db.astyanax;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertTrue;

public class DefaultSlabAllocatorTest {

    private static final Logger _log = LoggerFactory.getLogger(DefaultSlabAllocatorTest.class);

    @Test
    public void allocateGreaterThanMaxEvents() {
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i = 0; i < Constants.MAX_SLAB_SIZE * 3 / 2; i++) {
            sizes.add(64);
        }
        DefaultSlabAllocator slabAllocator = new DefaultSlabAllocator(mock(LifeCycleRegistry.class), mock(ManifestPersister.class), "metrics", mock(MetricRegistry.class));
        try {
            slabAllocator.allocate("mychannel", Constants.MAX_SLAB_SIZE * 3 / 2, Iterators.peekingIterator(sizes.iterator()));
            _log.info("SUCCESS: No exception thrown when " + (Constants.MAX_SLAB_SIZE * 3 / 2) + " events allocated");
        } catch (Exception e) {
            _log.error("ERROR: " + e.getClass().getName() + " thrown when " + (Constants.MAX_SLAB_SIZE * 3 / 2) + " events allocated");
            assert (false);
        }
    }

    @Test
    public void allocateMaxEvents() {
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i = 0; i < Constants.MAX_SLAB_SIZE; i++) {
            sizes.add(64);
        }
        DefaultSlabAllocator slabAllocator = new DefaultSlabAllocator(mock(LifeCycleRegistry.class), mock(ManifestPersister.class), "metrics", mock(MetricRegistry.class));
        try {
            slabAllocator.allocate("mychannel", Constants.MAX_SLAB_SIZE, Iterators.peekingIterator(sizes.iterator()));
            _log.info("SUCCESS: No exception thrown when " + Constants.MAX_SLAB_SIZE + " events allocated");
        } catch (Exception e) {
            _log.error("ERROR: " + e.getClass().getName() + " thrown when " + Constants.MAX_SLAB_SIZE + " events allocated");
            assert (false);
        }
    }

    @Test
    public void allocateFewerThanMaxEvents() {
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i = 0; i < Constants.MAX_SLAB_SIZE / 2; i++) {
            sizes.add(64);
        }
        DefaultSlabAllocator slabAllocator = new DefaultSlabAllocator(mock(LifeCycleRegistry.class), mock(ManifestPersister.class), "metrics", mock(MetricRegistry.class));
        try {
            slabAllocator.allocate("mychannel", Constants.MAX_SLAB_SIZE / 2, Iterators.peekingIterator(sizes.iterator()));
            _log.info("SUCCESS: No exception thrown when " + (Constants.MAX_SLAB_SIZE / 2) + " events allocated");
        } catch (Exception e) {
            _log.error("ERROR: " + e.getClass().getName() + " thrown when " + (Constants.MAX_SLAB_SIZE / 2) + " events allocated");
            assert (false);
        }
    }


    @Test
    public void computeAllocationGreaterThanMaxEvents() {
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i = 0; i < Constants.MAX_SLAB_SIZE * 3 / 2; i++) {
            sizes.add(64);
        }
        try {
            Pair<Integer, Integer> allocation = DefaultSlabAllocator.defaultAllocationCount(0, 0, Iterators.peekingIterator(sizes.iterator()));
            assertTrue(allocation.getLeft() == Constants.MAX_SLAB_SIZE, "ERROR: allocation has " + allocation.getLeft() + " slots, " + Constants.MAX_SLAB_SIZE + " expected");
            _log.info("SUCCESS: allocation has " + allocation.getLeft() + " slots, " + Constants.MAX_SLAB_SIZE + " expected when " + 0 + " slots already used and " + (Constants.MAX_SLAB_SIZE * 3 / 2) + " requested");
        } catch (Exception e) {
            _log.error("ERROR: " + e.getClass().getName() + " thrown when " + (Constants.MAX_SLAB_SIZE * 3 / 2) + " events allocated");
            assert (false);
        }
    }

    @Test
    public void computeAllocationMaxEvents() {
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i = 0; i < Constants.MAX_SLAB_SIZE; i++) {
            sizes.add(64);
        }
        try {
            Pair<Integer, Integer> allocation = DefaultSlabAllocator.defaultAllocationCount(0, 0, Iterators.peekingIterator(sizes.iterator()));
            assertTrue(allocation.getLeft() == Constants.MAX_SLAB_SIZE, "ERROR: allocation has " + allocation.getLeft() + " slots, " + Constants.MAX_SLAB_SIZE + " expected");
            _log.info("SUCCESS: allocation has " + allocation.getLeft() + " slots, " + Constants.MAX_SLAB_SIZE + " expected when " + 0 + " slots already used and " + Constants.MAX_SLAB_SIZE + " requested");
        } catch (Exception e) {
            _log.error("ERROR: " + e.getClass().getName() + " thrown when " + Constants.MAX_SLAB_SIZE + " events allocated");
            assert (false);
        }
    }

    @Test
    public void computeAllocationFewerThanMaxEvents() {
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i = 0; i < Constants.MAX_SLAB_SIZE / 2; i++) {
            sizes.add(64);
        }
        try {
            Pair<Integer, Integer> allocation = DefaultSlabAllocator.defaultAllocationCount(0, 0, Iterators.peekingIterator(sizes.iterator()));
            assertTrue(allocation.getLeft() == Constants.MAX_SLAB_SIZE / 2, "ERROR: allocation has " + allocation.getLeft() + " slots, " + (Constants.MAX_SLAB_SIZE / 2) + " expected");
            _log.info("SUCCESS: allocation has " + allocation.getLeft() + " slots, " + (Constants.MAX_SLAB_SIZE / 2) + " expected when " + 0 + " slots already used and " + (Constants.MAX_SLAB_SIZE / 2) + " requested");
        } catch (Exception e) {
            _log.error("ERROR: " + e.getClass().getName() + " thrown when " + (Constants.MAX_SLAB_SIZE / 2) + " events allocated");
            assert (false);
        }
    }

    @Test
    public void computeAllocationTotalMaxEvents() {
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i = 0; i < Constants.MAX_SLAB_SIZE / 2; i++) {
            sizes.add(64);
        }
        try {
            Pair<Integer, Integer> allocation = DefaultSlabAllocator.defaultAllocationCount(Constants.MAX_SLAB_SIZE / 2, 0, Iterators.peekingIterator(sizes.iterator()));
            assertTrue(allocation.getLeft() == Constants.MAX_SLAB_SIZE / 2, "ERROR: allocation has " + allocation.getLeft() + " slots, " + (Constants.MAX_SLAB_SIZE / 2) + " expected");
            _log.info("SUCCESS: allocation has " + allocation.getLeft() + " slots, " + (Constants.MAX_SLAB_SIZE / 2) + " expected when " + (Constants.MAX_SLAB_SIZE / 2) + " slots already used and " + Constants.MAX_SLAB_SIZE / 2 + " requested");
        } catch (Exception e) {
            _log.error("ERROR: " + e.getClass().getName() + " thrown when " + (Constants.MAX_SLAB_SIZE / 2) + " events allocated");
            assert (false);
        }
    }

    @Test
    public void computeAllocationTotalGreaterThanMaxEvents() {
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i = 0; i < Constants.MAX_SLAB_SIZE; i++) {
            sizes.add(64);
        }
        try {
            Pair<Integer, Integer> allocation = DefaultSlabAllocator.defaultAllocationCount(Constants.MAX_SLAB_SIZE / 2, 0, Iterators.peekingIterator(sizes.iterator()));
            assertTrue(allocation.getLeft() == Constants.MAX_SLAB_SIZE / 2, "ERROR: allocation has " + allocation.getLeft() + " slots, " + (Constants.MAX_SLAB_SIZE / 2) + " expected");
            _log.info("SUCCESS: allocation has " + allocation.getLeft() + " slots, " + (Constants.MAX_SLAB_SIZE / 2) + " expected when " + (Constants.MAX_SLAB_SIZE / 2) + " slots already used and " + Constants.MAX_SLAB_SIZE + " requested");
        } catch (Exception e) {
            _log.error("ERROR: " + e.getClass().getName() + " thrown when " + (Constants.MAX_SLAB_SIZE / 2) + " events allocated");
            assert (false);
        }
    }

    @Test
    public void computeAllocationTotalGreaterThanMaxEventsWithOversizeEvent() {
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i = 0; i < Constants.MAX_SLAB_SIZE - 1; i++) {
            sizes.add(64);
        }
        sizes.add(Constants.MAX_EVENT_SIZE_IN_BYTES + 1);
        try {
            Pair<Integer, Integer> allocation = DefaultSlabAllocator.defaultAllocationCount(Constants.MAX_SLAB_SIZE / 2, 0, Iterators.peekingIterator(sizes.iterator()));
            _log.error("ERROR: allocation did not throw exceptoion for oversize event");
        } catch (Exception e) {
            _log.info("SUCCESS: " + e.getClass().getName() + " thrown when " + (Constants.MAX_SLAB_SIZE / 2) + " events allocated");
        }
    }

}
