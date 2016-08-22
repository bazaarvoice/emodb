package com.bazaarvoice.emodb.common.cassandra.cqldriver;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

public class SelectedHostLoadBalancingPolicy extends RoundRobinPolicy implements LoadBalancingPolicy {
    // Note: - This method is called for every query to choose the hosts that the query should run on.
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
        if (statement instanceof SelectedHostStatement) {
            List<Host> hosts = Lists.newArrayListWithExpectedSize(1);
            Host host = ((SelectedHostStatement) statement).getHostCordinator();
            hosts.add(host);
            // _log.debug("Host selected for the query plan: ", host.getAddress());
            return hosts.iterator();
        } else {
            // use the RoundRobinPolicy's query plan for any other statement types.
            return super.newQueryPlan(loggedKeyspace, statement);
            // throw new UnsupportedOperationException("This load balancing policy is only for running the statements of type 'SelectedHostStatement'.");
        }
    }
}

