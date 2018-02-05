package com.bazaarvoice.emodb.common.zookeeper.leader;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.dropwizard.lifecycle.Managed;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sample implementation of a {@link PartitionedLeaderService}.  This service does nothing but log when leadership
 * is gained or lost and maintain a metric of how many partitions it is currently leading.  Useful for testing.
 */
public class LoggingPartitionedService extends PartitionedLeaderService implements Managed {

    private final Logger _log = LoggerFactory.getLogger(getClass());

    private final String _serviceName;
    private final AtomicInteger _ownedPartitions;

    public LoggingPartitionedService(CuratorFramework curator, String leaderPath, String instanceId, String serviceName,
                                     int numPartitions, long reacquireDelay, long repartitionDelay, TimeUnit delayUnit,
                                     MetricRegistry metricRegistry, @Nullable Clock clock) {
        super(curator, leaderPath, instanceId, serviceName, numPartitions, reacquireDelay, repartitionDelay, delayUnit, clock);
        setServiceFactory(this::createServiceForPartition);
        
        _serviceName = serviceName;
        _ownedPartitions = new AtomicInteger(0);
        metricRegistry.register(
                MetricRegistry.name("bv.emodb.LoggingPartitionedService", serviceName),
                (Gauge) _ownedPartitions::intValue);
    }

    private Service createServiceForPartition(final int partition) {
        return new AbstractService() {
            private ExecutorService _service;
            private CountDownLatch _latch = new CountDownLatch(1);

            @Override
            protected void doStart() {
                _service = Executors.newSingleThreadExecutor(
                        new ThreadFactoryBuilder().setNameFormat(String.format("%s-%d-%%d", _serviceName, partition)).build());
                _service.submit(() -> {
                    _log.info("{}-{}: Service started", _serviceName, partition);
                    _ownedPartitions.incrementAndGet();
                    try {
                        while (!_service.isShutdown()) {
                            try {
                                _latch.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                if (!_service.isShutdown()) {
                                    _log.info("{}-{}: Service thread interrupted prior to shutdown", _serviceName, partition);
                                }
                            }
                        }
                    } finally {
                        _log.info("{}-{}: Service terminating", _serviceName, partition);
                        _ownedPartitions.decrementAndGet();
                    }
                });
                notifyStarted();
            }

            @Override
            protected void doStop() {
                _latch.countDown();
                _service.shutdown();
                notifyStopped();
            }
        };
    }
}
