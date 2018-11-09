package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.bazaarvoice.emodb.sor.DataStoreZooKeeper;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;

import static com.google.common.base.Preconditions.checkNotNull;

public class MinSplitSizeCleanupMonitor extends LeaderService {

    private static final String SERVICE_NAME = "min-split-size-cleanup-montor";
    private static final String LEADER_DIR = "/leader/min-split-size";

    @Inject
    public MinSplitSizeCleanupMonitor(@DataStoreZooKeeper CuratorFramework curator, @SelfHostAndPort HostAndPort selfHostAndPort,
                                      LeaderServiceTask leaderServiceTask, LifeCycleRegistry lifecycle,
                                      @MinSplitSizeMap MapStore<DataStoreMinSplitSize> minSplitSizeMap) {
        super(curator, LEADER_DIR, selfHostAndPort.toString(), SERVICE_NAME, 1, TimeUnit.MINUTES,
                () -> new MigratorCleanupService(minSplitSizeMap));
        leaderServiceTask.register(SERVICE_NAME, this);
        lifecycle.manage(new ManagedGuavaService(this));
    }

    private static class MigratorCleanupService extends AbstractScheduledService {

        private final MapStore<DataStoreMinSplitSize> _minSplitSizeMap;

        public MigratorCleanupService(MapStore<DataStoreMinSplitSize> minSplitSizeMap) {
            _minSplitSizeMap = checkNotNull(minSplitSizeMap);
        }

        @Override
        protected void runOneIteration() throws Exception {
            Map<String, DataStoreMinSplitSize> minSplitSizes = _minSplitSizeMap.getAll();
            for (Map.Entry<String, DataStoreMinSplitSize> entry : minSplitSizes.entrySet()) {
                if (entry.getValue().getExpirationTime().isBefore(Instant.now())) {
                    _minSplitSizeMap.remove(entry.getKey());
                }
            }
        }

        /**
         *
         * @return a schedule that causes this service to be executed once per day at midnight
         */
        @Override
        protected Scheduler scheduler() {

            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

            return Scheduler.newFixedRateSchedule(
                    Duration.between(now, OffsetDateTime.of(now.toLocalDate().plusDays(1).atStartOfDay(), ZoneOffset.UTC)).getSeconds(),
                    Duration.ofDays(1).getSeconds(), TimeUnit.SECONDS);
        }
    }
}
