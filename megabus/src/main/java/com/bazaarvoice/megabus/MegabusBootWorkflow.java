package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.google.common.util.concurrent.AbstractService;
import static com.google.common.base.Preconditions.checkNotNull;

public class MegabusBootWorkflow extends AbstractService {

    private KafkaCluster _kafkaCluster;
    private MegabusBootDAO _megabusBootDAO;
    private String _applicationId;
    private Topic _megabusTopic;

    public MegabusBootWorkflow(KafkaCluster kafkaCluster, MegabusBootDAO megabusBootDAO, String applicationId, Topic megabusTopic) {
        _kafkaCluster = checkNotNull(kafkaCluster);
        _megabusBootDAO = checkNotNull(megabusBootDAO);
        _applicationId = checkNotNull(applicationId);
        _megabusTopic = checkNotNull(megabusTopic);
    }

    @Override
    protected void doStart() {
        switch (_megabusBootDAO.getBootStatus(_applicationId)) {
            case NOT_STARTED:
                _megabusBootDAO.initiateBoot(_applicationId, _megabusTopic);
            // Scan Stash status is now updated synchronously before the wait times.
            // so once the scan starts, the status we get here should be in progress and this should avoid multiple initiate boots.
            case IN_PROGRESS:
            case COMPLETE:
                notifyStarted();
                return;
            case FAILED:
                notifyFailed(new RuntimeException("Megabus Boot Failed"));
        }
    }

    @Override
    protected void doStop() {
        notifyStopped();
    }
}
