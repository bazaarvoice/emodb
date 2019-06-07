package com.bazaarvoice.megabus.resolver;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.megabus.MegabusApplicationId;
import com.bazaarvoice.megabus.MegabusBootDAO;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class DocumentResolverManager implements Managed {

    private final MegabusBootDAO _statusDAO;
    private final ScheduledExecutorService _bootService;
    private final MegabusRefResolver _megabusRefResolver;
    private final String _applicationId;

    @Inject
    public DocumentResolverManager(LifeCycleRegistry lifeCycle, MegabusBootDAO statusDAO,
                                   MegabusRefResolver megabusRefResolver, @MegabusApplicationId String applicationId) {
        _statusDAO = checkNotNull(statusDAO, "statusDAO");
        _megabusRefResolver = checkNotNull(megabusRefResolver);
        _applicationId = checkNotNull(applicationId);
        _bootService = Executors.newSingleThreadScheduledExecutor();
        lifeCycle.manage(this);
    }


    @Override
    public void start() throws Exception {
        _bootService.scheduleAtFixedRate(() -> {
            if (_statusDAO.getBootStatus(_applicationId) == MegabusBootDAO.BootStatus.COMPLETE) {
                _megabusRefResolver.startAsync();
                _bootService.shutdown();
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    @Override
    public void stop() throws Exception {
        _bootService.shutdownNow();
        _megabusRefResolver.stopAsync();
        _megabusRefResolver.awaitTerminated(10, TimeUnit.SECONDS);
    }
}
