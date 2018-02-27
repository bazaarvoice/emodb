package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.google.inject.Inject;

/** Starts the Database master fanout thread. */
public class MasterFanout {

    @Inject
    public MasterFanout(LifeCycleRegistry lifeCycle, final FanoutManager fanoutManager) {
        lifeCycle.manage(fanoutManager.newMasterFanout());

        // Until both of the following are true we need to continue running the legacy master fanout:
        // 1. All servers writing to the legacy un-partitioned master queue have been taken out of service
        // 2. All events previously written to the master fanout have been processed and acknowledged
        lifeCycle.manage(fanoutManager.newLegacyMasterFanout());
    }
}
