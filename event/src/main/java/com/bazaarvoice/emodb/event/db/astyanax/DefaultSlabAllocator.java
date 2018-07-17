package com.bazaarvoice.emodb.event.db.astyanax;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.event.core.MetricsGroupName;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import io.dropwizard.lifecycle.ExecutorServiceManager;
import io.dropwizard.util.Duration;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultSlabAllocator implements SlabAllocator {
    private final ManifestPersister _persister;
    private final LoadingCache<String, ChannelAllocationState> _channelStateCache;
    private final Meter _inactiveSlabMeter;

    @Inject
    public DefaultSlabAllocator(LifeCycleRegistry lifeCycle, ManifestPersister persister, @MetricsGroupName String metricsGroup, MetricRegistry metricRegistry) {
        _persister = persister;
        _channelStateCache = CacheBuilder.newBuilder()
                // Close each slab after a period of inactivity well before the reader slab timeout.  We need to be
                // careful about causing race conditions when closing slabs--if a slab is closed while there's any
                // chance writers are still writing, we could lose events.  So we only close slabs once (a) they're
                // full or (b) they haven't been accessed in a while.  We don't close slabs at shutdown because it's
                // hard to be completely sure all writers have quiesced.
                .expireAfterAccess(Constants.OPEN_SLAB_MARKER_TTL.toMillis() / 2, TimeUnit.MILLISECONDS)
                .removalListener(new RemovalListener<String, ChannelAllocationState>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, ChannelAllocationState> notification) {
                        closeChannelState(notification.getValue());
                    }
                })
                .build(new CacheLoader<String, ChannelAllocationState>() {
                    @Override
                    public ChannelAllocationState load(String channel) throws Exception {
                        return new ChannelAllocationState();
                    }
                });
        // Cleanup the cache once a minute to ensure channels are closed soon after they become inactive so when
        // readers detect "slate" open slabs it's because a writer has crashed, not that the writer is relatively idle.
        defaultExecutor(lifeCycle, metricsGroup).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                _channelStateCache.cleanUp();
            }
        }, 1, 1, TimeUnit.MINUTES);
        _inactiveSlabMeter = metricRegistry.meter(MetricRegistry.name(metricsGroup, "DefaultSlabAllocator", "inactive_slabs"));
    }

    private static ScheduledExecutorService defaultExecutor(LifeCycleRegistry lifeCycle, String metricsGroup) {
        String nameFormat = "Events Slab Allocator Cleanup-" + metricsGroup.substring(metricsGroup.lastIndexOf('.') + 1) + "-%d";
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, threadFactory);
        lifeCycle.manage(new ExecutorServiceManager(executor, Duration.seconds(5), nameFormat));
        return executor;
    }

    @Override
    public SlabAllocation allocate(String channelName, int desiredCount) {
        checkNotNull(channelName, "channelName");
        checkArgument(desiredCount > 0, "desiredCount must be >0");

        ChannelAllocationState channelState = _channelStateCache.getUnchecked(channelName);

        // Rotate slabs on a periodic basis so slab entries don't expire after the manifest
        channelState.rotateIfNecessary();

        // Scenarios:
        // - Slab is open and has space.  (Any open slab must have space.)
        //   - Synchronously allocate from remaining space and return.
        // - Slab is closed and requester wants >= MAX_SLAB_SIZE events
        //   - Generate a private slab ID, persist it, return the entire slab.
        // - Slab is closed and requester wants < MAX_SLAB_SIZE events
        //   - Synchronously generate slab ID, persist it, allocate from remaining space and return.

        if (desiredCount >= Constants.MAX_SLAB_SIZE) {
            // Special case for callers writing lots and lots of events.  They don't have to synchronize on the
            // SlabPersister I/O operation around creating slabs.

            // Is there is an existing open slab we can allocate from?
            SlabAllocation allocation = channelState.allocate(desiredCount);
            if (allocation != null) {
                return allocation;
            }

            // No existing slab.  Create a new slab just for the caller and not shared with anyone else.
            SlabRef slab = createSlab(channelName);
            return new DefaultSlabAllocation(slab, 0, Constants.MAX_SLAB_SIZE);

        } else {
            // Regular case for callers writing a few events.  Allocate from an existing open slab if possible, and if
            // not synchronize on the SlabPersister I/O operation of creating a new slab so only one slab is created
            // per channel at a time.
            synchronized (channelState.getSlabCreationLock()) {
                // Can we satisfy the allocation without creating a new slab?
                SlabAllocation allocation = channelState.allocate(desiredCount);
                if (allocation != null) {
                    return allocation;
                }

                // Must create a new slab.
                SlabRef slab = createSlab(channelName);

                // We're still guaranteed that the channel is closed (!channel.isAttached()) because all calls to the
                // channel.attach() method are protected by the SlabCreationLock which we held when checking isAttached.
                return channelState.attachAndAllocate(slab, desiredCount);
            }
        }
    }

    private void closeChannelState(ChannelAllocationState channelState) {
        SlabRef slab = channelState.detach();
        if (slab != null) {
            slab.release();
            _inactiveSlabMeter.mark();
        }
    }

    private SlabRef createSlab(String channel) {
        return new SlabRef(channel, generateSlabId(), _persister);
    }

    private ByteBuffer generateSlabId() {
        return TimeUUIDSerializer.get().toByteBuffer(TimeUUIDs.newUUID());
    }
}
