package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.google.common.util.concurrent.AbstractService;
import static com.google.common.base.Preconditions.checkNotNull;

public class MegabusBookWorkflow extends AbstractService {

    private KafkaCluster _kafkaCluster;
    private MegabusBootDAO _megabusBootDAO;
    private String _applicationId;
    private Topic _megabusTopic;

    public MegabusBookWorkflow(KafkaCluster kafkaCluster, MegabusBootDAO megabusBootDAO, String applicationId, Topic megabusTopic) {
        _kafkaCluster = checkNotNull(kafkaCluster);
        _megabusBootDAO = checkNotNull(megabusBootDAO);
        _applicationId = checkNotNull(applicationId);
        _megabusTopic = checkNotNull(megabusTopic);
    }

    @Override
    protected void doStart() {
        _kafkaCluster.createTopicIfNotExists(_megabusTopic);

        switch (_megabusBootDAO.getBootStatus(_applicationId)) {
            case NOT_STARTED:
                _megabusBootDAO.initiateBoot(_applicationId, _megabusTopic);
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

    }
}
