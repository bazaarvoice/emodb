package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.kafka.Topic;

public interface MegabusBootDAO {

    enum BootStatus {
        NOT_STARTED,
        IN_PROGRESS,
        FAILED,
        COMPLETE
    }

    void initiateBoot(String applicationId, Topic topic);

    BootStatus getBootStatus(String applicationId);
}
