package com.bazaarvoice.megabus.resolver;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.zookeeper.store.GuavaServiceController;
import com.bazaarvoice.megabus.MegabusApplicationId;
import com.bazaarvoice.megabus.MegabusBootDAO;
import com.bazaarvoice.megabus.service.ResilientService;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.google.common.base.Preconditions.checkNotNull;

public class MegabusRefResolverManager extends AbstractScheduledService {

    private static final Logger _log = LoggerFactory.getLogger(ResilientService.class);


    private final MegabusBootDAO _statusDAO;
    private final MegabusRefResolver _megabusRefResolver;
    private final MissingRefDelayProcessor _missingRefDelayProcessor;
    private final String _applicationId;

    @Inject
    public MegabusRefResolverManager(LifeCycleRegistry lifeCycle, MegabusBootDAO statusDAO,
                                   MegabusRefResolver megabusRefResolver, MissingRefDelayProcessor missingRefDelayProcessor,
                                   @MegabusApplicationId String applicationId) {
        _statusDAO = checkNotNull(statusDAO, "statusDAO");
        _megabusRefResolver = checkNotNull(megabusRefResolver);
        _missingRefDelayProcessor = checkNotNull(missingRefDelayProcessor);
        _applicationId = checkNotNull(applicationId);
        lifeCycle.manage(new ManagedGuavaService(this));
    }

    @Override
    protected void runOneIteration() throws Exception {
        MegabusBootDAO.BootStatus bootStatus = null;
        try {
            bootStatus = _statusDAO.getBootStatus(_applicationId);
        } catch (Throwable t) {
            _log.error("Failed to retrieve boot status.", t);
        }

        if (bootStatus == MegabusBootDAO.BootStatus.COMPLETE) {
           _megabusRefResolver.startAsync();
           _missingRefDelayProcessor.startAsync();
           stopAsync();
        }
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, 10, TimeUnit.SECONDS);
    }

    @Override
    protected void shutDown() throws Exception {
        _missingRefDelayProcessor.stopAsync();
        _megabusRefResolver.stopAsync();
    }
}
