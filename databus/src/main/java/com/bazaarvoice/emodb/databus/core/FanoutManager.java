package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.databus.repl.ReplicationSource;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import io.dropwizard.lifecycle.Managed;

public interface FanoutManager {

    /** Starts the main fanout thread that copies from __system_bus:master to individual subscriptions. */
    Managed newMasterFanout();
    
    /** Starts polling remote data centers and copying events to local individual subscriptions. */
    Managed newInboundReplicationFanout(DataCenter dataCenter, ReplicationSource replicationSource);
}
