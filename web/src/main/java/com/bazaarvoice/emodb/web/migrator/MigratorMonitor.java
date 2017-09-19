package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.web.migrator.migratorstatus.MigratorStatusDAO;
import com.bazaarvoice.emodb.web.scanner.ScannerZooKeeper;
import com.bazaarvoice.emodb.web.scanner.control.ScanWorkflow;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.TimeUnit;

/**
 * Monitor which starts and stops the LocalMigratorMonitor based on leader election.
 */
public class MigratorMonitor extends LeaderService {

    private static final String SERVICE_NAME = "migrator-monitor";
    private static final String LEADER_DIR = "/leader/monitor";

    @Inject
    public MigratorMonitor(@ScannerZooKeeper CuratorFramework curator, @SelfHostAndPort HostAndPort selfHostAndPort,
                           final ScanWorkflow workflow, final MigratorStatusDAO statusDAO,
                           final DataTools dataTools, LifeCycleRegistry lifecycle, LeaderServiceTask leaderServiceTask,
                           MetricRegistry metricRegistry) {
        super(curator, LEADER_DIR, selfHostAndPort.toString(), SERVICE_NAME, 1, TimeUnit.MINUTES,
                new Supplier<Service>() {
                    @Override
                    public Service get() {
                        return new LocalMigratorMonitor(workflow, statusDAO, dataTools);
                    }
                });

        ServiceFailureListener.listenTo(this, metricRegistry);
        leaderServiceTask.register(SERVICE_NAME, this);
        lifecycle.manage(new ManagedGuavaService(this));
    }
}
