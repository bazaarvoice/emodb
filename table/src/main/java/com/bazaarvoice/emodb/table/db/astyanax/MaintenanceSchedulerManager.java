package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.common.zookeeper.store.GuavaServiceController;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.table.db.TableChangesEnabled;
import com.bazaarvoice.emodb.table.db.curator.TableMutexManager;
import com.bazaarvoice.emodb.table.db.generic.CachingTableDAORegistry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Starts the leader service that dispatches table maintenance tasks in the current data center.
 */
public class MaintenanceSchedulerManager {
    @Inject
    public MaintenanceSchedulerManager(LifeCycleRegistry lifeCycle,
                                       final MaintenanceDAO tableDao,
                                       final Optional<TableMutexManager> tableMutexManager,
                                       @CurrentDataCenter final String selfDataCenter,
                                       @CachingTableDAORegistry final CacheRegistry cacheRegistry,
                                       @Maintenance final CuratorFramework curator,
                                       @SelfHostAndPort final HostAndPort self,
                                       final LeaderServiceTask dropwizardTask,
                                       final MoveTableTask moveTableTask,
                                       @TableChangesEnabled ValueStore<Boolean> tableChangesEnabled,
                                       @Maintenance final String scope,
                                       final MetricRegistry metricRegistry) {
        final Supplier<Service> maintenanceServiceFactory = new Supplier<Service>() {
            @Override
            public Service get() {
                return new MaintenanceScheduler(tableDao, tableMutexManager, selfDataCenter, cacheRegistry, moveTableTask);
            }
        };

        // Now start the maintenance scheduler subject to winning a leader election.
        Supplier<LeaderService> leaderServiceFactory = new Supplier<LeaderService>() {
            @Override
            public LeaderService get() {
                LeaderService service = new LeaderService(
                        curator, "/leader/table-maintenance", self.toString(),
                        "Leader-TableMaintenance-" + scope, 1, TimeUnit.MINUTES,
                        maintenanceServiceFactory);
                ServiceFailureListener.listenTo(service, metricRegistry);
                dropwizardTask.register(scope.toLowerCase() + "-maintenance", service);
                return service;
            }
        };

        // Turn everything on and off based on a boolean flag in ZooKeeper.
        lifeCycle.manage(new GuavaServiceController(tableChangesEnabled, leaderServiceFactory));
    }
}
