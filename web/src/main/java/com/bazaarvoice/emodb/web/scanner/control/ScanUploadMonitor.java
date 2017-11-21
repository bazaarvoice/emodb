package com.bazaarvoice.emodb.web.scanner.control;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.web.scanner.ScannerZooKeeper;
import com.bazaarvoice.emodb.web.scanner.notifications.ScanCountListener;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatusDAO;
import com.bazaarvoice.emodb.web.scanner.writer.ScanWriterGenerator;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.TimeUnit;

/**
 * Monitor which starts and stops the LocalScanUploadMonitor based on leader election.
 */
public class ScanUploadMonitor extends LeaderService {

    private static final String SERVICE_NAME = "scan-upload-monitor";
    private static final String LEADER_DIR = "/leader/monitor";

    @Inject
    public ScanUploadMonitor(@ScannerZooKeeper CuratorFramework curator, @SelfHostAndPort HostAndPort selfHostAndPort,
                             final ScanWorkflow scanWorkflow, final ScanStatusDAO scanStatusDAO,
                             final ScanWriterGenerator scanWriterGenerator,
                             final StashStateListener stashStateListener, final ScanCountListener scanCountListener,
                             final DataTools dataTools, LifeCycleRegistry lifecycle, LeaderServiceTask leaderServiceTask,
                             MetricRegistry metricRegistry) {
        super(curator, LEADER_DIR, selfHostAndPort.toString(), SERVICE_NAME, 1, TimeUnit.MINUTES,
                new Supplier<Service>() {
                    @Override
                    public Service get() {
                        return new LocalScanUploadMonitor(scanWorkflow, scanStatusDAO, scanWriterGenerator,
                                stashStateListener, scanCountListener, dataTools);
                    }
                });

        ServiceFailureListener.listenTo(this, metricRegistry);
        leaderServiceTask.register(SERVICE_NAME, this);
        lifecycle.manage(new ManagedGuavaService(this));
    }
}
