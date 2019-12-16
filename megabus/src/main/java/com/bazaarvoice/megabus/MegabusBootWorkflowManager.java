package com.bazaarvoice.megabus;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.megabus.guice.MegabusTopic;
import com.bazaarvoice.megabus.guice.MegabusZookeeper;
import com.bazaarvoice.megabus.guice.TableEventRegistrationService;
import com.bazaarvoice.megabus.refproducer.MegabusRefProducerManager;
import com.bazaarvoice.megabus.guice.MegabusRefResolverService;
import com.bazaarvoice.megabus.guice.MissingRefDelayService;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MegabusBootWorkflowManager implements Managed {

    private static final Logger _log = LoggerFactory.getLogger(MegabusBootWorkflowManager.class);


    private static final String LEADER_DIR = "/leader/megabus";

    private final Service _bootInitiater;
    private final Service _refResolverService;
    private final Service _missingRefDelayService;
    private final Service _tableEventRegistrationService;
    private final Managed _refProducerManager;
    private final MegabusBootDAO _megabusBootDAO;
    private final ScheduledExecutorService _bootCoordinator;

    private final String _applicationId;


    @Inject
    public MegabusBootWorkflowManager(@MegabusZookeeper CuratorFramework curator,
                                      @SelfHostAndPort HostAndPort selfHostAndPort,
                                      LifeCycleRegistry lifeCycle,
                                      LeaderServiceTask leaderServiceTask,
                                      @MegabusRefResolverService Service refResolverService,
                                      @MissingRefDelayService Service missingRefDelayService,
                                      @TableEventRegistrationService Service tableEventRegistrationService,
                                      MegabusBootDAO megabusBootDAO,
                                      @MegabusApplicationId String megabusApplicationId,
                                      @MegabusTopic Topic megabusTopic,
                                      MegabusRefProducerManager refProducerManager,
                                      MegabusBootDAO statusDAO) {
        _applicationId = megabusApplicationId;
        _bootInitiater = new LeaderService(curator, LEADER_DIR, selfHostAndPort.toString(),
                MegabusBootInitiater.SERVICE_NAME, 1, TimeUnit.MINUTES,
                () -> new MegabusBootInitiater(megabusBootDAO, megabusApplicationId, megabusTopic, refProducerManager));
        _megabusBootDAO = megabusBootDAO;
        _missingRefDelayService = missingRefDelayService;
        _refResolverService = refResolverService;
        _tableEventRegistrationService = tableEventRegistrationService;
        _refProducerManager = refProducerManager;
        _bootCoordinator = Executors.newSingleThreadScheduledExecutor();
        lifeCycle.manage(this);
    }

    @Override
    public void start() throws Exception {
        _tableEventRegistrationService.startAsync().awaitRunning();
        _bootInitiater.startAsync().awaitRunning();

        _bootCoordinator.scheduleAtFixedRate(() -> {
            MegabusBootDAO.BootStatus bootStatus = null;
            try {
                bootStatus = _megabusBootDAO.getBootStatus(_applicationId);
            } catch (Throwable t) {
                _log.error("Failed to retrieve boot status.", t);
            }

            if (bootStatus == MegabusBootDAO.BootStatus.COMPLETE) {
                _missingRefDelayService.startAsync();
                _refResolverService.startAsync();
                try {
                    _log.info("starting ref producer manager");
                    _refProducerManager.start();
                } catch (Exception e) {
                    _log.error("Partitioned Ref Producer failed to start", e);
                }
                _bootCoordinator.shutdown();
            }
        }, 0, 15, TimeUnit.SECONDS);
    }

    @Override
    public void stop() throws Exception {
        _refResolverService.stopAsync();
        _missingRefDelayService.stopAsync();
        _refProducerManager.stop();

        _refResolverService.awaitTerminated();
        _missingRefDelayService.awaitTerminated();
    }
}
