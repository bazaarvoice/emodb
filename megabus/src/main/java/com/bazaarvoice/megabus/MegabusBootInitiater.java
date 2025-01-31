package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.megabus.refproducer.MegabusRefProducerManager;
import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public class MegabusBootInitiater extends AbstractIdleService {

    private static final Logger _log = LoggerFactory.getLogger(MegabusBootInitiater.class);

    public static final String SERVICE_NAME = "megabus-boot-initiater";

    private final MegabusBootDAO _megabusBootDAO;
    private final String _applicationId;
    private final Topic _megabusTopic;
    private final MegabusRefProducerManager _megabusRefProducerManager;

    public MegabusBootInitiater(MegabusBootDAO megabusBootDAO, String applicationId, Topic megabusTopic,
                                MegabusRefProducerManager megabusRefProducerManager) {
        _megabusBootDAO = requireNonNull(megabusBootDAO);
        _applicationId = requireNonNull(applicationId);
        _megabusTopic = requireNonNull(megabusTopic);
        _megabusRefProducerManager = requireNonNull(megabusRefProducerManager);
    }

    @Override
    protected void startUp() {
        _megabusRefProducerManager.createRefSubscriptions();
        _log.info("Starting up serice {} in MegabusBootInitiater with application ID {}", SERVICE_NAME, _applicationId);
        switch (_megabusBootDAO.getBootStatus(_applicationId)) {
            case NOT_STARTED:
                _megabusBootDAO.initiateBoot(_applicationId, _megabusTopic);
            // Scan Stash status is now updated synchronously before the wait times.
            // so once the scan starts, the status we get here should be in progress and this should avoid multiple initiate boots.
            case IN_PROGRESS:
            case COMPLETE:
                return;
            case FAILED:
                throw new RuntimeException("Megabus Boot Failed");
        }
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
