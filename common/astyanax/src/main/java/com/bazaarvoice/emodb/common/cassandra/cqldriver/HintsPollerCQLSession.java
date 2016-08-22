package com.bazaarvoice.emodb.common.cassandra.cqldriver;

import com.bazaarvoice.emodb.common.cassandra.CqlCluster;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.datastax.driver.core.Session;
import io.dropwizard.lifecycle.Managed;

/**
 * Dropwizard managed connection pool for Hints Poller
 */
public class HintsPollerCQLSession implements Managed {
    public static final String SYSTEM_KEYSPACE = "system";

    private final CqlCluster _cluster;
    private Session _cqlSession;

    public HintsPollerCQLSession(LifeCycleRegistry lifeCycle, CqlCluster cluster) {
        _cluster = cluster;
        lifeCycle.manage(this);
    }

    public void start()
            throws Exception {
        _cqlSession = _cluster.connect(SYSTEM_KEYSPACE);
    }

    public void stop()
            throws Exception {
        if (_cqlSession != null) {
            _cqlSession.close();
        }
    }

    public Session getCqlSession() {
        return _cqlSession;
    }
}
