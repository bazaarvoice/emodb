package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.google.inject.Inject;

/** Starts the Database master fanout thread. */
public class MasterFanout {

    @Inject
    public MasterFanout(LifeCycleRegistry lifeCycle, final FanoutManager fanoutManager) {
        lifeCycle.manage(new ManagedGuavaService(fanoutManager.newMasterFanout()));
    }
}
