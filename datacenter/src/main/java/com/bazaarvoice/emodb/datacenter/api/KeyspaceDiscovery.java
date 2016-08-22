package com.bazaarvoice.emodb.datacenter.api;

import java.util.Collection;

public interface KeyspaceDiscovery {
    Collection<String> getKeyspacesForDataCenter(String cassandraDataCenter);
}
