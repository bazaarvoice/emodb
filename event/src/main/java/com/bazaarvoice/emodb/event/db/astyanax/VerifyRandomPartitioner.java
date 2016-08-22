package com.bazaarvoice.emodb.event.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.apache.cassandra.dht.RandomPartitioner;

import java.io.IOException;

public class VerifyRandomPartitioner implements Managed {
    private final CassandraKeyspace _keyspace;

    @Inject
    public VerifyRandomPartitioner(LifeCycleRegistry lifeCycle, CassandraKeyspace keyspace) {
        _keyspace = keyspace;
        lifeCycle.manage(this);
    }

    @Override
    public void start() throws Exception {
        // Not using the RandomPartitioner could lead to hotspots in the Cassandra ring.
        _keyspace.warnIfPartitionerMismatch(RandomPartitioner.class);
    }

    @Override
    public void stop() throws IOException {
        // Do nothing
    }
}
